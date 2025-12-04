package com.morpheusdata.proxmox.ve.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.projection.CloudPoolIdentity
import com.morpheusdata.model.projection.DatastoreIdentity
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import groovy.util.logging.Slf4j
import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil

/**
 * @author Neil van Rensburg
 */

@Slf4j
class PoolSync {

    private Cloud cloud
    ProxmoxVePlugin plugin
    private MorpheusContext morpheusContext
    private HttpApiClient apiClient
    private Map authConfig


    public PoolSync(ProxmoxVePlugin plugin, Cloud cloud, HttpApiClient apiClient) {
        this.@plugin = plugin
        this.@cloud = cloud
        this.@morpheusContext = plugin.morpheus
        this.@apiClient = apiClient
        this.@authConfig = plugin.getAuthConfig(cloud)
    }


    def execute() {
        log.debug "PoolSync"
        try {
            def listResults = ProxmoxApiComputeUtil.listProxmoxPools(apiClient, plugin.getAuthConfig(cloud))
            log.debug("Pools found: $listResults.data")

            if (listResults.success) {
                def cloudItems = listResults?.data
                def domainRecords = morpheusContext.async.cloud.pool.listIdentityProjections(cloud.id, "proxmox.pool.${cloud.id}", null)

                SyncTask<CloudPoolIdentity, Map, CloudPool> syncTask = new SyncTask<>(domainRecords, cloudItems as Collection)
                syncTask.addMatchFunction { CloudPoolIdentity domainObject, Map cloudItem ->
                    domainObject.externalId == cloudItem.poolid
                }.onAdd { itemsToAdd ->
                    addMissingPools(itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItem<CloudPool, Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<CloudPoolIdentity, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheusContext.async.cloud.pool.listById(updateItems?.collect { it.existingItem.id }).map { CloudPool cloudPool ->
                        return new SyncTask.UpdateItem<CloudPool, Map>(existingItem: cloudPool, masterItem: updateItemMap[cloudPool.id].masterItem)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<CloudPool, Map>> updateItems ->
                    //nothing here...
                }.onDelete { removeItems ->
                    morpheusContext.async.cloud.pool.bulkRemove(removeItems).blockingGet()
                }.start()
            }
        } catch (e) {
            log.error("PoolSync error: ${e}", e)
        }
    }


    private addMissingPools(Collection<Map> addList) {
        log.info("PoolSync:addMissingPools: addList.size(): ${addList.size()}")
        def poolAdds = []
        try {
            addList?.each { cloudItem ->
                def saveConfig = [
                        owner     : cloud.owner,
                        name       : cloudItem.poolid,
                        externalId : cloudItem.poolid,
                        uniqueId   : cloudItem.poolid,
                        internalId : cloudItem.poolid,
                        refType    : 'ComputeZone',
                        refId      : cloud.id,
                        cloud      : cloud,
                        category   : "proxmox.pool.${cloud.id}",
                        code       : "proxmox.pool.${cloud.id}.${cloudItem.poolid}",
                        active     : true
                ]
                poolAdds << new CloudPool(saveConfig)
            }
            log.info("Adding Pools: $poolAdds")
            morpheusContext.services.cloud.pool.bulkCreate(poolAdds)
        } catch (e) {
            log.error "Error adding Pool ${e}", e
        }
    }

}
