package com.morpheusdata.proxmox.ve.sync

import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeCapacityInfo
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerType
import com.morpheusdata.model.OsType
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.proxmox.ve.util.ProxmoxMiscUtil
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class HostSync {

    private Cloud cloud
    private MorpheusContext morpheusContext
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private Map authConfig
    private String hostUID
    private String hostPWD

    /**
     * @author Neil van Rensburg
     */

    HostSync(ProxmoxVePlugin proxmoxVePlugin, Cloud cloud, HttpApiClient apiClient) {
        this.@plugin = proxmoxVePlugin
        this.@cloud = cloud
        this.@morpheusContext = proxmoxVePlugin.morpheus
        this.@apiClient = apiClient
        this.@authConfig = plugin.getAuthConfig(cloud)

        this.@hostUID = cloud.configMap.hostUsername
        this.@hostPWD = cloud.configMap.hostPassword
    }


    def execute() {
        log.debug "Execute HostSync STARTED: ${cloud.id}"

        try {
            def hostListResults = ProxmoxApiComputeUtil.listProxmoxHypervisorHosts(apiClient, authConfig)
            log.debug("Host list results: $hostListResults")

            if (hostListResults.success) {
                def cloudItems = hostListResults?.data

                def domainRecords = morpheusContext.async.computeServer.listIdentityProjections(cloud.id, null).filter {
                    ComputeServerIdentityProjection projection ->
                    if (projection.category == "proxmox.ve.host.${cloud.id}") {
                        return true
                    }
                    false
                }

                SyncTask<ComputeServerIdentityProjection, Map, ComputeServer> syncTask = new SyncTask<>(domainRecords, cloudItems)
                syncTask.addMatchFunction { ComputeServerIdentityProjection domainObject, Map cloudItem ->
                    domainObject.externalId == cloudItem?.node
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheusContext.async.computeServer.listById(updateItems?.collect { it.existingItem.id }).map { ComputeServer server ->
                        return new SyncTask.UpdateItem<ComputeServer, Map>(existingItem: server, masterItem: updateItemMap[server.id].masterItem)
                    }
                }.onAdd { itemsToAdd ->
                    addMissingHosts(cloud, itemsToAdd)
                }.onUpdate { List<SyncTask.UpdateItem<ComputeServer, Map>> updateItems ->
                    updateMatchedHosts(cloud, updateItems)
                }.onDelete { removeItems ->
                    removeMissingHosts(cloud, removeItems)
                }.start()
            } else {
                log.error "Error in getting hosts : ${hostListResults}"
            }
        } catch(e) {
            log.error "Error in HostSync execute : ${e}", e
        }
        log.debug "Execute HostSync COMPLETED: ${cloud.id}"
    }


    private addMissingHosts(Cloud cloud, Collection<Map> addList) {
        log.debug "addMissingHosts: ${cloud} ${addList.size()}"
        def serverType = new ComputeServerType(code: 'proxmox-ve-node')
        def serverOs = new OsType(code: 'linux')

        for (cloudItem in addList) {
            try {
                log.info("Adding cloud host: $cloudItem with IP $cloudItem.ipAddress")
                
                // Handle null values with safe defaults for offline nodes
                def maxCpu = cloudItem.maxcpu ?: 0
                def maxMem = cloudItem.maxmem ?: 0
                def usedMem = cloudItem.mem ?: 0
                def maxDisk = cloudItem.maxdisk ?: 0
                def usedDisk = cloudItem.disk ?: 0
                def usedCpu = cloudItem.cpu ?: 0
                def usedCpuPercent = usedCpu * 100
                
                def serverConfig = [
                        account          : cloud.owner,
                        category         : "proxmox.ve.host.${cloud.id}",
                        cloud            : cloud,
                        name             : cloudItem.node,
                        resourcePool     : null,
                        externalId       : cloudItem.node,
                        uniqueId         : "${cloud.id}.${cloudItem.node}",
                        sshHost          : cloudItem.ipAddress,
                        sshUsername      : hostUID,
                        sshPassword      : hostPWD,
                        status           : 'provisioned',
                        provision        : false,
                        serverType       : 'hypervisor',
                        computeServerType: serverType,
                        serverOs         : serverOs,
                        osType           : 'linux',
                        hostname         : cloudItem.node,
                        externalIp       : cloudItem.ipAddress,
                        powerState       : (cloudItem.status == 'online') ? ComputeServer.PowerState.on : ComputeServer.PowerState.off
                ]

                ComputeCapacityInfo capacityInfo = new ComputeCapacityInfo()

                Map capacityFieldValueMap = [
                        maxCores   : maxCpu.toLong(),
                        maxStorage : maxDisk.toLong(),
                        usedStorage: usedDisk.toLong(),
                        maxMemory  : maxMem.toLong(),
                        usedMemory : usedMem.toLong(),
                        usedCpu    : usedCpuPercent.toLong(),
                ]

                ComputeServer newServer = new ComputeServer(serverConfig)
                ProxmoxMiscUtil.doUpdateDomainEntity(capacityInfo, capacityFieldValueMap)
                newServer.capacityInfo = capacityInfo
                log.debug("Adding Compute Server: $serverConfig")
                if (!morpheusContext.async.computeServer.bulkCreate([newServer]).blockingGet()){
                    log.error "Error in creating host server ${newServer}"
                }

            } catch(e) {
                log.error "Error in creating host: ${e}", e
            }
        }
    }


    private updateMatchedHosts(Cloud cloud, List<SyncTask.UpdateItem<ComputeServer, Map>> updateItems) {
        log.info("Updating ${updateItems.size()} Hosts...")
        def updates = []

        try {
            for (def updateItem in updateItems) {
                def existingItem = updateItem.existingItem
                def cloudItem = updateItem.masterItem
                def doUpdate = false

                ComputeCapacityInfo capacityInfo = existingItem.getComputeCapacityInfo() ?: new ComputeCapacityInfo()

                // Handle null values with safe defaults for offline nodes
                def maxCpu = cloudItem.maxcpu ?: 0
                def maxMem = cloudItem.maxmem ?: 0
                def usedMem = cloudItem.mem ?: 0
                def maxDisk = cloudItem.maxdisk ?: 0
                def usedDisk = cloudItem.disk ?: 0
                def usedCpu = cloudItem.cpu  ?: 0
                def usedCpuPercent = usedCpu * 100

                Map serverFieldValueMap = [
                        account     : cloud.owner,
                        category    : "proxmox.ve.host.${cloud.id}",
                        cloud       : cloud,
                        name        : cloudItem.node,
                        resourcePool: null,
                        uniqueId    : "${cloud.id}.${cloudItem.node}",
                        //sshHost     : cloudItem.ipAddress,
                        //sshUsername : hostUID,
                        //sshPassword : hostPWD,
                        hostname    : cloudItem.hostName ?: cloudItem.node,
                        externalIp  : cloudItem.ipAddress,
                        maxCores    : maxCpu.toLong(),
                        maxStorage  : maxDisk.toLong(),
                        usedStorage : usedDisk.toLong(),
                        maxMemory   : maxMem.toLong(),
                        usedMemory  : usedMem.toLong(),
                        usedCpu     : usedCpuPercent.toLong(),
                        powerState  : (cloudItem.status == 'online') ? ComputeServer.PowerState.on : ComputeServer.PowerState.off
                ]

                Map capacityFieldValueMap = [
                        maxCores   : maxCpu.toLong(),
                        maxStorage : maxDisk.toLong(),
                        usedStorage: usedDisk.toLong(),
                        maxMemory  : maxMem.toLong(),
                        usedMemory : usedMem.toLong(),
                        usedCpu    : usedCpuPercent.toLong(),
                ]

                if (ProxmoxMiscUtil.doUpdateDomainEntity(existingItem, serverFieldValueMap) ||
                        ProxmoxMiscUtil.doUpdateDomainEntity(capacityInfo, capacityFieldValueMap)) {
                    existingItem.capacityInfo = capacityInfo
                    updates << existingItem
                }
            }

            if (updates) morpheusContext.async.computeServer.bulkSave(updates).blockingGet()

        } catch(e) {
            log.warn("error updating host stats: ${e}", e)
        }

        //Examples:
        // Nutanix - https://github.com/gomorpheus/morpheus-nutanix-prism-plugin/blob/api-1.1.x/src/main/groovy/com/morpheusdata/nutanix/prism/plugin/sync/HostsSync.groovy
        // XCP-ng - https://github.com/gomorpheus/morpheus-xenserver-plugin/blob/main/src/main/groovy/com/morpheusdata/xen/sync/HostSync.groovy
        // Openstack - https://github.com/gomorpheus/morpheus-openstack-plugin/blob/main/src/main/groovy/com/morpheusdata/openstack/plugin/sync/HostsSync.groovy
    }


    private removeMissingHosts(Cloud cloud, List<ComputeServerIdentityProjection> removeList) {
        log.debug("Remove Hosts...")
        morpheusContext.async.computeServer.bulkRemove(removeList).blockingGet()
    }
}