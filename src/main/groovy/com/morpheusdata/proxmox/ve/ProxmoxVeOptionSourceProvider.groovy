package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.ImageType
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class ProxmoxVeOptionSourceProvider extends AbstractOptionSourceProvider {

    ProxmoxVePlugin plugin
    MorpheusContext morpheusContext

    ProxmoxVeOptionSourceProvider(ProxmoxVePlugin plugin, MorpheusContext context) {
        this.plugin = plugin
        this.morpheusContext = context
    }

    @Override
    MorpheusContext getMorpheus() {
        return this.morpheusContext
    }

    @Override
    Plugin getPlugin() {
        return this.plugin
    }

    @Override
    String getCode() {
        return 'proxmox-ve-option-source'
    }

    @Override
    String getName() {
        return 'Proxmox VE Option Source'
    }

    @Override
    List<String> getMethodNames() {
        return new ArrayList<String>(['proxmoxVeProvisionImage', 'proxmoxVeNode'])
    }


    def proxmoxVeNode(args) {
        log.debug "proxmoxVeNode: ${args}"
        def cloudId = args?.size() > 0 ? args.getAt(0).zoneId.toLong() : null
        def options = []

        def domainRecords = morpheusContext.async.computeServer.listIdentityProjections(cloudId, null).filter {
            ComputeServerIdentityProjection projection ->
                if (projection.category == "proxmox.ve.host.$cloudId") {
                    return true
                }
                false
        }.blockingSubscribe() {
            options << [name: it.name, value: it.externalId]
        }

        if (options.size() > 0) {
            options = options.sort { it.name }
        }

        log.error("FOUND ${options.size()} ComputeServer Nodes...")
        return options
    }


    def proxmoxVeProvisionImage(args) {
        log.debug "proxmoxVeProvisionImage: ${args}"
        def cloudId = args?.size() > 0 ? args.getAt(0).zoneId.toLong() : null
        def accountId = args?.size() > 0 ? args.getAt(0).accountId.toLong() : null
        def locationExternalIds = []

        def options = []
        def invalidStatus = ['Saving', 'Failed', 'Converting']
        def syncedVirtualImageLocations = morpheusContext.async.virtualImage.location.listIdentityProjections(
                new DataQuery().
                        withFilter('refId', cloudId).
                        withFilter('category', 'proxmox.image')
        ).blockingSubscribe() {
            //if (it.deleted == false &&
            //    !(it.status in invalidStatus)) {
            if (morpheusContext.services.virtualImage.listById([it.virtualImage.id]).first().userUploaded) {
                options << [name: "$it.virtualImage.name (Uploaded)", value: it.virtualImage.id]
            } else {
                options << [name: it.virtualImage.name, value: it.virtualImage.id]
            }
                locationExternalIds << it.externalId
            log.debug("External ID found: $it.externalId")
            //}
        }

        ImageType[] imageTypes = [ImageType.qcow2]
        def virtualImageIds = morpheusContext.async.virtualImage.listIdentityProjections(accountId, imageTypes).filter {
            it.deleted == false
        }.map{it.id}.toList().blockingGet()

        if(virtualImageIds.size() > 0) {

            def query = new DataQuery().withFilters([
                    new DataFilter('status', 'Active'),
                    new DataFilter('id', 'in', virtualImageIds),
                    new DataFilter('userUploaded', true)
            ])

            morpheusContext.async.virtualImage.list(query).blockingSubscribe {
                if (!(it.externalId in locationExternalIds)) {
                    log.debug("Uploaded External ID found: $it.externalId ($it.name)")
                    options << [name: "$it.name (To Be Uploaded)", value: it.id]
                }
            }
        }

        if (options.size() > 0) {
            options = options.sort { it.name }
        }

        log.error("FOUND ${options.size()} VirtualImages...")
        return options
    }
}
