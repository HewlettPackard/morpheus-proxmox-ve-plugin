package com.morpheusdata.proxmox.ve.sync


import com.morpheusdata.proxmox.ve.util.ProxmoxApiComputeUtil
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Account
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ImageType
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.VirtualImageLocation
import com.morpheusdata.model.projection.VirtualImageIdentityProjection
import com.morpheusdata.model.projection.VirtualImageLocationIdentityProjection
import com.morpheusdata.proxmox.ve.ProxmoxVePlugin
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author Neil van Rensburg
 */

@Slf4j
class VirtualImageLocationSync {


    private Cloud cloud
    private MorpheusContext context
    private ProxmoxVePlugin plugin
    private HttpApiClient apiClient
    private CloudProvider cloudProvider
    private Map authConfig


    VirtualImageLocationSync(ProxmoxVePlugin proxmoxVePlugin, Cloud cloud, HttpApiClient apiClient, CloudProvider cloudProvider) {
        this.@plugin = proxmoxVePlugin
        this.@cloud = cloud
        this.@apiClient = apiClient
        this.@context = proxmoxVePlugin.morpheus
        this.@cloudProvider = cloudProvider
        this.@authConfig = plugin.getAuthConfig(cloud)
    }


    def execute() {
        try {
            log.debug "Execute VirtualImageLocationSync STARTED: ${cloud.id}"
            def cloudItems = ProxmoxApiComputeUtil.listTemplates(apiClient, authConfig).data
            log.debug("Proxmox templates found: $cloudItems")

            Observable domainRecords = context.async.virtualImage.location.listIdentityProjections(new DataQuery().withFilters([
                    new DataFilter("refType", "ComputeZone"),
                    new DataFilter("refId", cloud.id)
            ]))

            Observable logRecords = context.async.virtualImage.location.listIdentityProjections(new DataQuery().withFilters([
                    new DataFilter("refType", "ComputeZone"),
                    new DataFilter("refId", cloud.id)
            ]))

            logRecords.blockingForEach { log.debug("Existing Location: ${it.imageName}($it.externalId})") }

            //domainRecords.each { record -> log.debug("Domain Record found: ${record.subscribe()}") }
            //log.debug("Domain Records: ${domainRecords}")
            SyncTask<VirtualImageLocationIdentityProjection, Map, VirtualImageLocation> syncTask = new SyncTask<>(domainRecords, cloudItems)

            syncTask.addMatchFunction { VirtualImageLocationIdentityProjection domainObject, Map cloudItem ->
                domainObject.externalId == cloudItem.vmid.toString()
            }.onAdd { List<Map> newItems ->
                addMissingVirtualImageLocations(newItems)
            }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<VirtualImageLocationIdentityProjection, VirtualImageLocation>> updateItems ->
                context.async.virtualImage.location.listById(updateItems.collect {it.existingItem.id } as List<Long>)
            }.onUpdate { List<SyncTask.UpdateItem<VirtualImageLocation, Map>> updateItems ->
                updateMatchedVirtualImageLocations(updateItems)
            }.onDelete { removeItems ->
                removeMissingVirtualImageLocations(removeItems)
            }.start()

        } catch (e) {
            log.error("Error in VirtualImageSync execute : ${e}", e)
        }
        log.debug("Execute VirtualImageSync COMPLETED: ${cloud.id}")
    }


    private addMissingVirtualImageLocations(Collection<Map> objList) {
        log.debug "addMissingVirtualImageLocations: ${objList?.size()}"

        def names = objList.collect{it.name}?.unique()
        List<VirtualImageIdentityProjection> existingItems = []
        def allowedImageTypes = ['qcow2']

        Observable domainRecords = context.async.virtualImage.listIdentityProjections(new DataQuery().withFilters([
                new DataFilter<String>("imageType", "in", allowedImageTypes),
                new DataFilter<Collection<String>>("name", "in", names),
                //new DataFilter<String>("category", "proxmox.image"),
                new DataOrFilter(
                        new DataFilter<Boolean>("systemImage", false),
                        new DataOrFilter(
                                new DataFilter("owner", null),
                                new DataFilter<Long>("owner.id", cloud.owner.id)
                        )
                )
        ]))

        log.debug("Virtual Image Identities:")
        domainRecords.blockingIterable().each {
            log.debug("Virtual Image Identity: $it.name($it.externalId)")
        }

        SyncTask<VirtualImageIdentityProjection, Map, VirtualImage> syncTask = new SyncTask<>(domainRecords, objList)
        syncTask.addMatchFunction { VirtualImageIdentityProjection domainObject, Map cloudItem ->
            domainObject.externalId && (domainObject.externalId == cloudItem.vmid.toString())
        }.addMatchFunction { VirtualImageIdentityProjection domainObject, Map cloudItem ->
            !domainObject.externalId && (domainObject.name == cloudItem.name)
        }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<VirtualImageIdentityProjection, Map>> updateItems ->
            Map<Long, SyncTask.UpdateItemDto<VirtualImageIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
            context.async.virtualImage.listById(updateItems?.collect { it.existingItem.id }).map { VirtualImage virtualImage ->
                SyncTask.UpdateItemDto<VirtualImageIdentityProjection, Map> matchItem = updateItemMap[virtualImage.id]
                return new SyncTask.UpdateItem<VirtualImage, Map>(existingItem: virtualImage, masterItem: matchItem.masterItem)
            }
        }.onAdd { itemsToAdd ->
            addMissingVirtualImages(itemsToAdd)
        }.onUpdate { List<SyncTask.UpdateItem<VirtualImage, Map>> updateItems ->
            // Found the VirtualImage for this location.. just need to create the location
            addMissingVirtualImageLocationsForImages(updateItems)
        }.onDelete { itemsToRemove ->
            // Nothing to do....
        }.start()
    }


    private addMissingVirtualImages(Collection<Map> addList) {
        log.debug "addMissingVirtualImages ${addList?.size()}"

        def adds = []
        addList.each {
            log.debug("Creating virtual image: $it")
            VirtualImage virtImg = new VirtualImage(buildVirtualImageConfig(it))
            VirtualImageLocation virtImgLoc = new VirtualImageLocation(buildLocationConfig(virtImg))
            virtImg.imageLocations = [virtImgLoc]
            adds << virtImg
        }

        log.debug "About to create ${adds.size()} virtualImages"
        context.async.virtualImage.create(adds, cloud).blockingGet()
    }


    private addMissingVirtualImageLocationsForImages(List<SyncTask.UpdateItem<VirtualImage, Map>> addItems) {
        log.debug "addMissingVirtualImageLocationsForImages ${addItems?.size()}"

        def locationAdds = []
        addItems?.each { add ->
            VirtualImage virtualImage = add.existingItem
            VirtualImageLocation location = new VirtualImageLocation(buildLocationConfig(virtualImage))
            locationAdds << location
        }

        if(locationAdds) {
            log.debug "About to create ${locationAdds.size()} locations"
            context.async.virtualImage.location.create(locationAdds, cloud).blockingGet()
        }
    }


    private updateMatchedVirtualImageLocations(List<SyncTask.UpdateItem<VirtualImageLocation, Map>> updateList) {
        log.debug "updateMatchedVirtualImages: $cloud.name($cloud.id) ${updateList.size()} images"
        def saveLocationList = []
        def saveImageList = []
        def virtualImagesById = context.async.virtualImage.listById(updateList.collect { it.existingItem.virtualImage.id }).toMap {it.id}.blockingGet()

        for (def updateItem in updateList) {
            def existingItem = updateItem.existingItem
            def virtualImage = virtualImagesById[existingItem.virtualImage.id]
            def cloudItem = updateItem.masterItem
            def virtualImageConfig = buildVirtualImageConfig(cloudItem)
            def save = false
            def saveImage = false
            def state = 'Active'

            def imageName = virtualImageConfig.name
            log.debug("Existing Name: $existingItem.imageName, New Name: $imageName")
            if(existingItem.imageName != imageName) {
                existingItem.imageName = imageName

                //?? What is this for?
                //if(virtualImage.imageLocations?.size() < 2) {
                    virtualImage.name = imageName
                    saveImage = true
                //}
                save = true
            }
            if(existingItem.externalId != virtualImageConfig.externalId) {
                existingItem.externalId = virtualImageConfig.externalId
                save = true
            }
            if(virtualImage.status != state) {
                virtualImage.status = state
                saveImageList << virtualImage
            }
            if (existingItem.imageRegion != cloud.regionCode) {
                existingItem.imageRegion = cloud.regionCode
                save = true
            }
            if (virtualImage.remotePath != virtualImageConfig.remotePath) {
                virtualImage.remotePath = virtualImageConfig.remotePath
                saveImage = true
            }
            if (virtualImage.imageRegion != virtualImageConfig.imageRegion) {
                virtualImage.imageRegion = virtualImageConfig.imageRegion
                saveImage = true
            }
            if (virtualImage.minDisk != virtualImageConfig.minDisk) {
                virtualImage.minDisk = virtualImageConfig.minDisk as Long
                saveImage = true
            }
            if (virtualImage.bucketId != virtualImageConfig.bucketId) {
                virtualImage.bucketId = virtualImageConfig.bucketId
                saveImage = true
            }
            if(virtualImage.systemImage == null) {
                virtualImage.systemImage = false
                saveImage = true
            }

            if(save) {
                saveLocationList << existingItem
            }

            if(saveImage) {
                saveImageList << virtualImage
            }
        }

        if(saveLocationList) {
            context.async.virtualImage.location.save(saveLocationList, cloud).blockingGet()
        }
        if(saveImageList) {
            context.async.virtualImage.save(saveImageList.unique(), cloud).blockingGet()
        }
    }


    private Map buildLocationConfig(VirtualImage image) {
        return [
                virtualImage: image,
                code        : "proxmox.ve.image.${cloud.id}.${image.externalId}",
                internalId  : image.internalId,
                externalId  : image.externalId,
                imageName   : image.name,
                imageRegion : cloud.regionCode,
                isPublic    : false,
                refType     : 'ComputeZone',
                refId       : cloud.id
        ]
    }


    private buildVirtualImageConfig(Map cloudItem) {
        Account account = cloud.account
        def regionCode = cloud.regionCode

        def imageConfig = [
                account             : account,
                category            : "proxmox.image",
                name                : cloudItem.name,
                code                : "proxmox.image.${cloudItem.vmid}",
                imageType           : ImageType.qcow2,
                status              : 'Active',
                minDisk             : cloudItem.maxdisk,
                minRam              : cloudItem.minRam,
                //isPublic   : false,
                externalId          : cloudItem.vmid,
                //imageRegion: regionCode,
                //systemImage: false,
                refType             : 'ComputeZone',
                refId               : "${cloud.id}",
                blockDeviceConfig   : cloudItem.datastores
        ]

        return imageConfig
    }


    private removeMissingVirtualImageLocations(List<VirtualImageLocationIdentityProjection> removeList) {
        log.debug "removeMissingVirtualImageLocations: ${removeList?.size()}"
        def virtualImagesById = context.async.virtualImage.listById(removeList.collect { it.virtualImage.id }).toMap {it.id}.blockingGet()
        log.debug("VirtualImages: " + virtualImagesById.toString())

        def removeVirtualImages = []

        removeList.each { removeItem ->
            log.debug("VirtualImage ID is: $removeItem.virtualImage.id")
            def virtualImage = virtualImagesById[removeItem.virtualImage.id]
            if (virtualImage.imageLocations.size() == 1 && !virtualImage.systemImage) {
                removeVirtualImages << virtualImage
            }
        }
        log.debug("Removing Locations: $removeList")
        context.async.virtualImage.location.bulkRemove(removeList).blockingGet()
        //removeVirtualImages.each {
            log.debug("Removing Virtual Images: $removeVirtualImages")
            context.async.virtualImage.bulkRemove(removeVirtualImages).blockingGet()
        //}
    }


    public clean() {

        Observable domainRecords = context.async.virtualImage.list(new DataQuery().withFilters([
                new DataFilter<String>("category", "proxmox.image"),
                new DataOrFilter(
                        new DataFilter<Boolean>("systemImage", false),
                        new DataOrFilter(
                                new DataFilter("owner", null),
                                new DataFilter<Long>("owner.id", cloud.owner.id)
                        )
                )
        ]))

        Collection<VirtualImageIdentityProjection> imagesToDelete = []
        domainRecords.blockingIterable().each {virtualImage ->
            if (virtualImage.imageLocations.size() == 1 && !virtualImage.systemImage) {
                log.info("Virtual Image To Remove: $virtualImage.name($virtualImage.externalId)")
                imagesToDelete << virtualImage
            }
        }

        context.async.virtualImage.bulkRemove(imagesToDelete).blockingGet()
    }
}
