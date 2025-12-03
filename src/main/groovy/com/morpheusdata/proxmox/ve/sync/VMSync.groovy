package com.morpheusdata.proxmox.ve.sync


import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeCapacityInfo
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.MetadataTag
import com.morpheusdata.model.MetadataTagType
import com.morpheusdata.model.OsType
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.proxmox.ve.util.ProxmoxMiscUtil
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class VMSync {

    private Cloud cloud
    private MorpheusContext context
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private CloudProvider cloudProvider
    private Map authConfig


    VMSync(ProxmoxVePlugin proxmoxVePlugin, Cloud cloud, HttpApiClient apiClient, CloudProvider cloudProvider) {
        this.@plugin = proxmoxVePlugin
        this.@cloud = cloud
        this.@apiClient = apiClient
        this.@context = proxmoxVePlugin.morpheus
        this.@cloudProvider = cloudProvider
        this.@authConfig = plugin.getAuthConfig(cloud)
    }



    def execute() {
        try {
            log.debug "Execute VMSync STARTED: ${cloud.id}"
            def cloudItems = ProxmoxApiComputeUtil.listVMs(apiClient, authConfig).data
            // Sync BOTH managed and unmanaged VMs
            def domainRecords = context.async.computeServer.listIdentityProjections(cloud.id, null).filter {
                it.computeServerTypeCode in ['proxmox-qemu-vm', 'proxmox-qemu-vm-unmanaged']
            }

            log.debug("VM cloudItems: ${cloudItems.collect { it.toString() }}")
            log.debug("VM domainObjects: ${domainRecords.map { "${it.externalId} - ${it.name} - ${it.computeServerTypeCode}" }.toList().blockingGet()}")

            SyncTask<ComputeServerIdentityProjection, Map, ComputeServer> syncTask = new SyncTask<>(domainRecords, cloudItems)
            syncTask.addMatchFunction { ComputeServerIdentityProjection domainObject, Map cloudItem ->
                domainObject.externalId == cloudItem.vmid.toString()
            }.onAdd { itemsToAdd ->
                addMissingVirtualMachines(cloud, itemsToAdd)
            }.onDelete { itemsToDelete ->
                removeMissingVMs(itemsToDelete)
            }.withLoadObjectDetails { updateItems ->
                Map<Long, SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return context.async.computeServer.listById(updateItems?.collect { it.existingItem.id }).map { ComputeServer server ->
                    return new SyncTask.UpdateItem<ComputeServer, Map>(existingItem: server, masterItem: updateItemMap[server.id].masterItem)
                } 
            }.onUpdate { itemsToUpdate ->
                updateMatchingVMs(itemsToUpdate)
            }.start()
        } catch(e) {
            log.error "Error in VMSync execute : ${e}", e
        }
        log.debug "Execute VMSync COMPLETED: ${cloud.id}"
    }


    private void addMissingVirtualMachines(Cloud cloud, Collection items) {
        log.info("Adding ${items.size()} new VMs for Proxmox cloud ${cloud.name}")

        def newVMs = []

        // Fixed: Get the hypervisor hosts (nodes) map, not VMs
        def hostIdentitiesMap = context.async.computeServer.listIdentityProjections(cloud.id, null).filter {
            it.computeServerTypeCode == 'proxmox-ve-node'
        }.toMap { it.externalId }.blockingGet()

        def computeServerType = cloudProvider.computeServerTypes.find {
            it.code == 'proxmox-qemu-vm-unmanaged'
        }

        items.each { Map cloudItem ->
            def parentServer = hostIdentitiesMap[cloudItem.node]
            if (!parentServer) {
                log.warn("Could not find parent server for VM ${cloudItem.name} on node ${cloudItem.node}")
            }
            def usedCpu = cloudItem.cpu  ?: 0
            def usedCpuPercent = usedCpu * 100

            def newVM = new ComputeServer(
                account          : cloud.account,
                externalId       : cloudItem.vmid.toString(),
                name             : cloudItem.name,
                externalIp       : cloudItem.ip,
                internalIp       : cloudItem.ip,
                //sshHost          : cloudItem.ip,
                //sshUsername      : 'root',
                provision        : false,
                cloud            : cloud,
                lvmEnabled       : false,
                managed          : false,
                serverType       : 'vm',
                status           : 'provisioned',
                uniqueId         : cloudItem.vmid.toString(),
                powerState       : cloudItem.status == 'running' ? ComputeServer.PowerState.on : ComputeServer.PowerState.off,
                maxMemory        : cloudItem.maxmem,
                maxCores         : cloudItem.maxCores,
                coresPerSocket   : cloudItem.coresPerSocket,
                usedStorage      : cloudItem.disk?.toLong(),
                usedMemory       : cloudItem.mem?.toLong(),
                usedCpu          : usedCpuPercent.toLong(),
                parentServer     : parentServer,
                osType           : 'unknown',
                serverOs         : new OsType(code: 'unknown'),
                category         : "proxmox.ve.vm.${cloud.id}",
                computeServerType: computeServerType
            )
            newVMs << newVM
        }
        
        if (newVMs) {
            context.async.computeServer.bulkCreate(newVMs).blockingGet()
            log.info("Successfully added ${newVMs.size()} VMs")
        }
    }


    private updateMatchingVMs(List<SyncTask.UpdateItem<ComputeServer, Map>> updateItems) {
        log.debug("Updating ${updateItems.size()} existing VMs")

        def updates = []
        
        try {
            for (def updateItem in updateItems) {
                def existingItem = updateItem.existingItem
                def cloudItem = updateItem.masterItem
                def needsUpdate = false

                ComputeCapacityInfo capacityInfo = existingItem.getComputeCapacityInfo() ?: new ComputeCapacityInfo()
                def usedCpu = cloudItem.cpu  ?: 0
                def usedCpuPercent = usedCpu * 100
                // Fix power state comparison
                def cloudPowerState = (cloudItem.status == 'running') ? ComputeServer.PowerState.on : ComputeServer.PowerState.off

                Map serverFieldValueMap = [
                        hostname   : cloudItem.name,
                        externalIp : cloudItem.ip,
                        internalIp : cloudItem.ip,
                        maxCores   : cloudItem.maxCores ?: cloudItem.maxcpu?.toLong(),
                        maxStorage : cloudItem.maxdisk?.toLong(),
                        usedStorage: cloudItem.disk?.toLong(),
                        maxMemory  : cloudItem.maxmem?.toLong(),
                        usedMemory : cloudItem.mem?.toLong(),
                        usedCpu    : usedCpuPercent.toLong(),
                        powerState : cloudPowerState
                ]

                Map capacityFieldValueMap = [
                        maxCores   : cloudItem.maxCores ?: cloudItem.maxcpu?.toLong(),
                        maxStorage : cloudItem.maxdisk?.toLong(),
                        usedStorage: cloudItem.disk?.toLong(),
                        maxMemory  : cloudItem.maxmem?.toLong(),
                        usedMemory : cloudItem.mem?.toLong(),
                        usedCpu    : usedCpuPercent.toLong(),
                ]

                if (ProxmoxMiscUtil.doUpdateDomainEntity(existingItem, serverFieldValueMap)) {
                    needsUpdate = true
                }
                
                if (ProxmoxMiscUtil.doUpdateDomainEntity(capacityInfo, capacityFieldValueMap)) {
                    existingItem.capacityInfo = capacityInfo
                    needsUpdate = true
                }
                
                if (needsUpdate) {
                    updates << existingItem
                }
            }
            
            if (updates) {
                context.async.computeServer.bulkSave(updates).blockingGet()
            }
        } catch(e) {
            log.error("Error updating VM properties and stats: ${e}", e)
        }
    }


    private removeMissingVMs(List<ComputeServerIdentityProjection> removeItems) {
        log.info("Removing ${removeItems.size()} VMs that no longer exist in Proxmox...")
        
        removeItems.each { vm ->
            log.info("Removing orphaned VM: ${vm.name} (ID: ${vm.id}, External ID: ${vm.externalId}, Type: ${vm.computeServerTypeCode})")
        }
        
        if (removeItems) {
            try {
                def result = context.async.computeServer.bulkRemove(removeItems).blockingGet()
                log.info("Bulk remove completed successfully")
            } catch (e) {
                log.error("Error removing VMs: ${e}", e)
            }
        }
    }
}