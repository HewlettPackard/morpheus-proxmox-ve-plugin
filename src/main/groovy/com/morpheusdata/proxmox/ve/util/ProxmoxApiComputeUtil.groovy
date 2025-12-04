package com.morpheusdata.proxmox.ve.util

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerInterface
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import org.apache.http.entity.ContentType
import groovy.json.JsonSlurper

/**
 * @author Neil van Rensburg
 */

@Slf4j
class ProxmoxApiComputeUtil {

    //static final String API_BASE_PATH = "/api2/json"
    static final Long API_CHECK_WAIT_INTERVAL = 2000


    static addVMNics(HttpApiClient client, Map authConfig, List<ComputeServerInterface> newNics, String node, String vmId) {
        try {
            def tokenCfg = getApiV2Token(authConfig).data
            def diskAddOpts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : [:],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            ]

            newNics.each { nic ->
                diskAddOpts.body["$nic.externalId"] = "bridge=$nic.network.externalId,model=e1000e"
            }

            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config",
                    null, null,
                    new HttpApiClient.RequestOptions(diskAddOpts),
                    'POST'
            )

            return results
        } catch (e) {
            log.error "Error Adding NICs: ${e}", e
            return ServiceResponse.error("Error Adding NICs: ${e}")
        }
    }


    static List<Map> getExistingVMInterfaces(HttpApiClient client, Map authConfig, String nodeId, String vmId) {

        def vmConfigInfo = callListApiV2(client, "nodes/$nodeId/qemu/$vmId/config", authConfig).data
        log.info("VM Config Info: $vmConfigInfo")
        def nicInterfaces = vmConfigInfo.findAll { k, v -> k ==~ /net\d+/ }.collect { k, v -> [ label: k, value: v] }
        return nicInterfaces
    }


    static removeNetworkInterfaces(HttpApiClient client, Map authConfig, List<ComputeServerInterface> deletedNics, String node, String vmId) {
        log.debug("deleteVolumes")
        def tokenCfg = getApiV2Token(authConfig).data
        def nicRemoveOpts = [
                headers  : [
                        'Content-Type'       : 'application/json',
                        'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body     : [
                        delete: ""
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: true
        ]

        try {
            def success = true
            def errorMsg = ""
            deletedNics.each { ComputeServerInterface nic ->
                nicRemoveOpts.body.delete = nic.externalId
                def nicRemoveResults = client.callJsonApi(
                        (String) authConfig.apiUrl,
                        "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config",
                        null, null,
                        new HttpApiClient.RequestOptions(nicRemoveOpts),
                        'PUT'
                )
                if (!nicRemoveResults.success) {
                    errorMsg += "$nicRemoveResults.error\n"
                    success = false
                }
            }
            return new ServiceResponse(success: success, msg: errorMsg)
        } catch (e) {
            log.error "Error removing VM Network Interface: ${e}", e
            return ServiceResponse.error("Error removing VM Network Interface: ${e}")
        }
    }


    static resizeVMDisk(HttpApiClient client, Map authConfig, StorageVolume updatedVolume, String node, String vmId) {
        def tokenCfg = getApiV2Token(authConfig).data
        def diskResizeOpts = [
                headers  : [
                        'Content-Type'       : 'application/json',
                        'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body     : [
                        disk: "$updatedVolume.deviceName",
                        size: "${updatedVolume.maxStorage as Long / 1024 / 1024 / 1024}G"
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: true
        ]

        def results = client.callJsonApi(
                (String) authConfig.apiUrl,
                "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/resize",
                null, null,
                new HttpApiClient.RequestOptions(diskResizeOpts),
                'PUT'
        )

        return results
    }


    static addVMDisks(HttpApiClient client, Map authConfig, List<StorageVolume> newVolumes, String node, String vmId) {
        try {
            def tokenCfg = getApiV2Token(authConfig).data
            def diskAddOpts = [
                headers  : [
                    'Content-Type'       : 'application/json',
                    'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                    'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body     : [
                    delete: ""
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: true
            ]

            newVolumes.each { vol ->
                def size = "${vol.maxStorage as Long / 1024 / 1024 / 1024}"
                diskAddOpts.body["$vol.deviceName"] = "${vol.datastore.externalId}:$size,size=${size}G"
            }

            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config",
                    null, null,
                    new HttpApiClient.RequestOptions(diskAddOpts),
                    'POST'
            )

            return results
        } catch (e) {
            log.error "Error Provisioning VM: ${e}", e
            return ServiceResponse.error("Error Provisioning VM: ${e}")
        }
    }


    static deleteVolumes(HttpApiClient client, Map authConfig, String node, String vmId, List<String> ids) {
        log.debug("deleteVolumes")
        def tokenCfg = getApiV2Token(authConfig).data
        def diskRemoveOpts = [
                headers  : [
                        'Content-Type'       : 'application/json',
                        'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body     : [
                        delete: ""
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: true
        ]

        try {
            def success = true
            def errorMsg = ""
            ids.each { String diskId ->
                diskRemoveOpts.body.delete = diskId
                log.debug("Delete request path: \n${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config")
                log.debug("Delete request body: \n$diskRemoveOpts")
                def diskRemoveResults = client.callJsonApi(
                        (String) authConfig.apiUrl,
                        "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config",
                        null, null,
                        new HttpApiClient.RequestOptions(diskRemoveOpts),
                        'PUT'
                )
                if (!diskRemoveResults.success) {
                    errorMsg += "$diskRemoveResults.error\n"
                    success = false
                }
            }
            return new ServiceResponse(success: success, msg: errorMsg)
        } catch (e) {
            log.error "Error removing VM disk: ${e}", e
            return ServiceResponse.error("Error removing VM disk: ${e}")
        }
    }


    static int getHighestScsiDisk(diskList) {
        def scsiDisks = diskList.findAll { it.label ==~ /scsi\d+/ }
        if (!scsiDisks) return -1

        def highest = scsiDisks.max { it.label.replace("scsi", "").toInteger() }
        return highest.label.replace("scsi", "").toInteger()
    }



    static resizeVM(HttpApiClient client, Map authConfig, String node, String vmId, Long cpu, Long ram, List<StorageVolume> volumes, List<ComputeServerInterface> nics) {
        log.debug("resizeVMCompute")
        Long ramValue = ram / 1024 / 1024

        def rootVolume = volumes.find {it.rootVolume }

        try {
            log.debug("Resize Boot Disk...")
            def initialTemlpateDisks = getExistingVMStorage(client, authConfig, node, vmId)
            def tokenCfg = getApiV2Token(authConfig).data
            def resizeOpts = [
                headers  : [
                        'Content-Type'       : 'application/json',
                        'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body     : [
                        disk  : "${initialTemlpateDisks[0].label}",
                        size  : "${rootVolume.maxStorage as Long / 1024 / 1024 / 1024}G",
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: true
            ]

            def resizeResults = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/resize",
                    null, null,
                    new HttpApiClient.RequestOptions(resizeOpts),
                    'PUT'
            )

            log.debug("Post deployment Resize results: $resizeResults")
            log.debug("Resize compute, add additional Disks...")
            def opts = [
                headers  : [
                        'Content-Type'       : 'application/json',
                        'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                ],
                body     : [
                        node  : node,
                        vcpus : cpu,
                        cores : cpu,
                        memory: ramValue
                ],
                contentType: ContentType.APPLICATION_JSON,
                ignoreSSL: true
            ]

            volumes.each { vol ->
                if (!vol.rootVolume) {
                    def size = "${vol.maxStorage as Long / 1024 / 1024 / 1024}"
                    opts.body["$vol.deviceName"] = "${vol.datastore.externalId}:$size,size=${size}G"
                }
            }

//            def counter = 0
//            targetNetworks.each {network ->
//                opts.body["net$counter"] = "bridge=$network,model=e1000e"
//                counter++
//            }

            nics.each { nic ->
                opts.body["$nic.externalId"] = "bridge=$nic.network.externalId,model=e1000e"
            }

            log.debug("Setting VM Compute Size $vmId on node $node...")
            def results = client.callJsonApi(
                (String) authConfig.apiUrl,
                "${authConfig.v2basePath}/nodes/$node/qemu/$vmId/config",
                null, null,
                new HttpApiClient.RequestOptions(opts),
                'POST'
            )

            return results
        } catch (e) {
            log.error "Error Provisioning VM: ${e}", e
            return ServiceResponse.error("Error Provisioning VM: ${e}")
        }
    }



    static cloneTemplate(HttpApiClient client, Map authConfig, String templateId, String name, String nodeId, ComputeServer server) {
        //Long vcpus, Long ram, List<StorageVolume> volumes, List<String> targetNetworks) {
        log.debug("cloneTemplate: $templateId")

        Long vcpus = server.maxCores
        Long ram = server.maxMemory
        List<StorageVolume> volumes = server.volumes
        List<ComputeServerInterface> nics = server.interfaces
        def rtn = new ServiceResponse(success: true)
        def nextId = callListApiV2(client, "cluster/nextid", authConfig).data
        log.debug("Next VM Id is: $nextId")
        StorageVolume rootVolume = volumes.find { it.rootVolume }

        try {
            def tokenCfg = getApiV2Token(authConfig).data
            rtn.data = []
            def opts = [
                    headers: [
                            'Content-Type': 'application/json',
                            'Cookie': "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body: [
                            newid: nextId,
                            node: nodeId,
                            vmid: templateId,
                            name: name,
                            full: true,
                            storage: "${rootVolume.datastore.externalId}"
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            ]

            log.debug("Cloning template $templateId to VM $name($nextId) on node $nodeId")
            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/$templateId/clone",
                    null, null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            def resultData = new JsonSlurper().parseText(results.content)

            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                rtn.data = resultData

                ServiceResponse cloneWaitResult = waitForCloneToComplete(new HttpApiClient(), authConfig, templateId, nextId, nodeId, 3600L)

                if (!cloneWaitResult?.success) {
                    return ServiceResponse.error("Error Provisioning VM. Wait for clone error: ${cloneWaitResult}")
                }

                log.debug("Resizing newly cloned VM. Spec: CPU: $vcpus,\n RAM: $ram,\n Volumes: $volumes,\n NICs: $nics")
                ServiceResponse rtnResize = resizeVM(new HttpApiClient(), authConfig, nodeId, nextId, vcpus, ram, volumes, nics)

                if (!rtnResize?.success) {
                    return ServiceResponse.error("Error Sizing VM Compute. Resize compute error: ${rtnResize}")
                }

                rtn.data.vmId = nextId
            } else {
                rtn.msg = "Provisioning failed: ${results.toMap()}"
                rtn.success = false
            }
        } catch(e) {
            log.error "Error Provisioning VM: ${e}", e
            return ServiceResponse.error("Error Provisioning VM: ${e}")
        }
        return rtn
    }




    static List<Map> getExistingVMStorage(HttpApiClient client, Map authConfig, String nodeId, String vmId) {

        def vmConfigInfo = callListApiV2(client, "nodes/$nodeId/qemu/$vmId/config", authConfig).data
        def validBootDisks = ["scsi0", "virtio0", "sata0", "ide0"]
        def bootEntries = vmConfigInfo.boot?.trim()?.replaceAll("order=", "")?.split(/[;,\s]+/)
        def bootDisk = ""

        def extractDiskKeys  = { config ->
            def diskPrefixes = ["scsi", "virtio", "sata", "ide"]
            def diskKeys = config.keySet().findAll { key ->
                diskPrefixes.any { prefix -> key ==~ /${prefix}\d+/ }
            }
            return diskKeys
        }
        List vmStorageList = []

        def vmDiskKeys = extractDiskKeys(vmConfigInfo)

        //Boot disk specified in config boot order
        bootEntries.each { String diskLabel ->
            if (!bootDisk && validBootDisks.contains(diskLabel)) {
                bootDisk = diskLabel
            }
        }

        if (!bootDisk) {
            //select boot disk based on proxmox disk names
            validBootDisks.each { String diskLabel ->
                if (!bootDisk && vmDiskKeys.contains(diskLabel)) {
                    bootDisk = diskLabel
                }
            }
        }

        if (!bootDisk) {
            throw new Exception("Boot disk for VM not found!")
        }

        vmStorageList << [ label: "$bootDisk", config: vmConfigInfo[bootDisk], isRoot: true ]
        vmDiskKeys.each { String diskLabel ->
            if (diskLabel != bootDisk) {
                vmStorageList << [ label: "$diskLabel", config: vmConfigInfo[diskLabel], isRoot: false ]
            }
        }

        return vmStorageList
    }


    static startVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("startVM")
        return actionVMStatus(client, authConfig, nodeId, vmId, "start")
    }

    static rebootVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("rebootVM")
        return actionVMStatus(client, authConfig, nodeId, vmId, "reboot")
    }

    static shutdownVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("shutdownVM")
        return actionVMStatus(client, authConfig, nodeId, vmId, "shutdown")
    }

    static stopVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("stopVM")
        return actionVMStatus(client, authConfig, nodeId, vmId, "stop")
    }

    static resetVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("resetVM")
        return actionVMStatus(client, authConfig, nodeId, vmId, "reset")
    }


    static actionVMStatus(HttpApiClient client, Map authConfig, String nodeId, String vmId, String action) {

        try {
            def tokenCfg = getApiV2Token(authConfig).data
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : [
                            vmid: vmId,
                            node: nodeId
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            ]

            log.debug("Post path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/status/$action/")
            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/status/$action/",
                    null, null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            return results
        } catch (e) {
            log.error "Error performing $action on VM: ${e}", e
            return ServiceResponse.error("Error performing $action on VM: ${e}")
        }
    }


    static destroyVM(HttpApiClient client, Map authConfig, String nodeId, String vmId) {
        log.debug("destroyVM")
        try {
            def tokenCfg = getApiV2Token(authConfig).data
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body: null,
                    ignoreSSL: true,
                    contentType: ContentType.APPLICATION_JSON,
            ]

            log.debug("Delete Opts: $opts")
            log.debug("Delete path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/")

            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/",
                    new HttpApiClient.RequestOptions(opts),
                    'DELETE'
            )

            log.debug("VM Delete Response Details: ${results.toMap()}")
            return results

        //TODO - check for non 200 response
        } catch (e) {
            log.error "Error Destroying VM: ${e}", e
            return ServiceResponse.error("Error Destroying VM: ${e}")
        }
    }


    static createImageTemplate(HttpApiClient client, Map authConfig, String imageName, String nodeId, int cpu, Long ram, String sourceUri = null) {
        log.debug("createImage: $imageName")

        def rtn = new ServiceResponse(success: true)
        def nextId = callListApiV2(client, "cluster/nextid", authConfig).data
        log.debug("Next VM Id is: $nextId")

        try {
            def tokenCfg = getApiV2Token(authConfig).data
            rtn.data = []
            def opts = [
                    headers  : [
                            'Content-Type'       : 'application/json',
                            'Cookie'             : "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    body     : [
                            vmid: nextId,
                            node: nodeId,
                            name: imageName.replaceAll(/\s+/, ''),
                            template: true
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            ]

            log.debug("Creating blank template for attaching qcow2...")
            log.debug("Path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/")
            def results = client.callJsonApi(
                    (String) authConfig.apiUrl,
                    "${authConfig.v2basePath}/nodes/$nodeId/qemu/",
                    null, null,
                    new HttpApiClient.RequestOptions(opts),
                    'POST'
            )

            def resultData = new JsonSlurper().parseText(results.content)
            if (results?.success && !results?.hasErrors()) {
                rtn.success = true
                rtn.data = resultData
                rtn.data.templateId = nextId
            } else {
                rtn.msg = "Template create failed: $results.data $results $results.errorCode $results.content"
                rtn.success = false
            }
        } catch (e) {
            log.error "Error Provisioning VM: ${e}", e
            return ServiceResponse.error("Error Provisioning VM: ${e}")
        }
        return rtn
    }


    static ServiceResponse waitForCloneToComplete(HttpApiClient client, Map authConfig, String templateId, String vmId, String nodeId, Long timeoutInSec) {
        Long timeout = timeoutInSec * 1000
        Long duration = 0
        log.debug("waitForCloneToComplete: $templateId")

        try {
            def tokenCfg = getApiV2Token(authConfig).data
            def opts = [
                    headers: [
                            'Content-Type': 'application/json',
                            'Cookie': "PVEAuthCookie=$tokenCfg.token",
                            'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            ]

            log.debug("Checking VM Status after clone template $templateId to VM $vmId on node $nodeId")
            log.debug("Path is: $authConfig.apiUrl${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/config")

            while (duration < timeout) {
                log.debug("Checking VM $vmId status on node $nodeId")
                def results = client.callJsonApi(
                        (String) authConfig.apiUrl,
                        "${authConfig.v2basePath}/nodes/$nodeId/qemu/$vmId/config",
                        null, null,
                        new HttpApiClient.RequestOptions(opts),
                        'GET'
                )

                if (!results.success) {
                    log.error("Error checking VM clone result status.")
                    return results
                }

                def resultData = new JsonSlurper().parseText(results.content)
                if (!resultData.data.containsKey("lock")) {
                    return results
                } else {
                    log.info("VM Still Locked, wait ${API_CHECK_WAIT_INTERVAL}ms and check again...")
                }
                sleep(API_CHECK_WAIT_INTERVAL)
                duration += API_CHECK_WAIT_INTERVAL
            }
            return new ServiceResponse(success: false, msg: "Timeout", data: "Timeout")
        } catch(e) {
            log.error "Error Checking VM Clone Status: ${e}", e
            return ServiceResponse.error("Error Checking VM Clone Status: ${e}")
        }
    }


    static ServiceResponse getProxmoxDatastoresById(HttpApiClient client, Map authConfig, List storageIds) {

        List<Map> filteredDS = listProxmoxDatastores(client, authConfig).data.findAll { storageIds.contains(it.storage) }

        return new ServiceResponse(success: true, data: filteredDS)
    }


    static ServiceResponse listProxmoxDatastores(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxDatastores...")

        var allowedDatastores = ["rbd", "cifs", "zfspool", "nfs", "lvmthin", "lvm", "cephfs", "iscsi", "dir"]
        Collection<Map> validDatastores = []
        ServiceResponse datastoreResults = callListApiV2(client, "storage", authConfig)
        def queryNode = getProxmoxHypervisorNodeIds(client, authConfig).data[0]

        datastoreResults.data.each { Map ds ->
            if (allowedDatastores.contains(ds.type)) {
                if (ds.containsKey("nodes")) {
                    //some pools don't belong to any node, but api path needs node for status details
                    queryNode = ((String) ds.nodes).split(",")[0]
                } else {
                    ds.nodes = "all"
                }

                try {
                    ServiceResponse dsInfoResponse = callListApiV2(client, "nodes/${queryNode}/storage/${ds.storage}/status", authConfig)
                    
                    if (dsInfoResponse.success && dsInfoResponse.data instanceof Map) {
                        Map dsInfo = dsInfoResponse.data as Map
                        ds.total = dsInfo.total ?: 0
                        ds.avail = dsInfo.avail ?: 0
                        ds.used = dsInfo.used ?: 0
                        ds.enabled = dsInfo.enabled ?: 0
                    } else {
                        // Handle case where API call fails (like for offline nodes)
                        log.warn("Failed to get storage status for ${ds.storage} on node ${queryNode}, using defaults")
                        ds.total = 0
                        ds.avail = 0
                        ds.used = 0
                        ds.enabled = 0
                    }

                    validDatastores << ds
                    
                } catch (Exception e) {
                    log.error("Error getting datastore status for ${ds.storage} on node ${queryNode}: ${e.message}")
                    // Set default values and include the datastore anyway
                    ds.total = 0
                    ds.avail = 0
                    ds.used = 0
                    ds.enabled = 0
                    validDatastores << ds
                }
            } else {
                log.warn("Storage ${ds} ignored...")
            }
        }

        return new ServiceResponse(success: true, data: validDatastores)
    }



    static ServiceResponse listProxmoxNetworks(HttpApiClient client, Map authConfig, uniqueIfaces = false) {
        log.debug("listProxmoxNetworks...")

        Collection<Map> networks = []
        List<String> hosts = getProxmoxHypervisorNodeIds(client, authConfig).data

        hosts.each { host ->
            try {
                ServiceResponse hostNetworks = callListApiV2(client, "nodes/$host/network", authConfig)
                if (hostNetworks.success && hostNetworks.data) {
                    hostNetworks.data.each { Map network ->
                        if (['bridge', 'vlan'].contains(network?.type)) {
                            network.networkAddress = ""
                            if (network?.cidr) {
                                network.networkAddress = ProxmoxMiscUtil.getNetworkAddress(network.cidr)
                            } else if (network?.address && network?.netmask) {
                                network.networkAddress = ProxmoxMiscUtil.getNetworkAddress("$network.address/$network.netmask")
                            }
                            network.host = host
                            networks << network
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to get networks for host ${host}: ${e.message}")
            }
        }

        try {
            ServiceResponse sdnNetworks = callListApiV2(client, "cluster/sdn/vnets", authConfig)
            if (sdnNetworks.success && sdnNetworks.data) {
                sdnNetworks.data.each { Map sdn ->
                    sdn.networkAddress = ''
                    sdn.iface = sdn.vnet
                    sdn.host = "all"
                    networks << sdn
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get SDN networks: ${e.message}")
        }

        if (uniqueIfaces) {
            Set seenIfaces = new HashSet<>()
            List<Map> uniqueNetworks = networks.findAll { map ->
                if (map.iface && !seenIfaces.contains(map.iface)) {
                    seenIfaces << map.iface
                    return true
                }
                return false
            }
            return new ServiceResponse(success: true, data: uniqueNetworks)
        }

        return new ServiceResponse(success: true, data: networks)
    }



    static ServiceResponse getTemplateById(HttpApiClient client, Map authConfig, Long templateId) {

        def resp = listTemplates(client, authConfig)
        def filteredTemplate = resp.data.find { it.vmid == templateId}

        return new ServiceResponse(success: resp.success, data: filteredTemplate)
    }


    static ServiceResponse listTemplates(HttpApiClient client, Map authConfig) {
        log.debug("API Util listTemplates")
        def vms = []
        def qemuVMs = callListApiV2(client, "cluster/resources", authConfig)
        qemuVMs.data.each { Map vm ->
            if (vm?.template == 1 && vm?.type == "qemu") {
                vm.ip = "0.0.0.0"
                def vmConfigInfo = callListApiV2(client, "nodes/$vm.node/qemu/$vm.vmid/config", authConfig)
                vm.maxCores = (vmConfigInfo?.data?.sockets?.toInteger() ?: 0) * (vmConfigInfo?.data?.cores?.toInteger() ?: 0)
                vm.coresPerSocket = vmConfigInfo?.data?.cores?.toInteger() ?: 0

                vm.datastores = vmConfigInfo.data?.findAll { k, v ->
                    // Match disk keys like 'virtio0', 'scsi0', 'ide1', etc.
                    k ==~ /^(virtio|scsi|ide|sata)\d+$/ && v instanceof String && v.contains(':')
                }.collect { k, v ->
                    // Extract the storage ID before the colon
                    v.split(':')[0]
                }.unique()

                vms << vm
            }
        }
        return new ServiceResponse(success: true, data: vms)
    }


    static ServiceResponse listVMs(HttpApiClient client, Map authConfig) {
        log.debug("API Util listVMs")
        def vms = []
        def qemuVMs = callListApiV2(client, "cluster/resources", authConfig)
        qemuVMs.data.each { Map vm ->
            if (vm?.template == 0 && vm?.type == "qemu") {
                def vmAgentInfo = callListApiV2(client, "nodes/$vm.node/qemu/$vm.vmid/agent/network-get-interfaces", authConfig)
                vm.ip = "0.0.0.0"
                if (vmAgentInfo.success) {
                    def results = vmAgentInfo.data?.result
                    results.each {
                        if (it."ip-address-type" == "ipv4" && it."ip-address" != "127.0.0.1" && vm.ip == "0.0.0.0") {
                            vm.ip = it."ip-address"
                        }
                    }
                }
                def vmConfigInfo = callListApiV2(client, "nodes/$vm.node/qemu/$vm.vmid/config", authConfig)
                vm.maxCores = (vmConfigInfo?.data?.data?.sockets?.toInteger() ?: 0) * (vmConfigInfo?.data?.data?.cores?.toInteger() ?: 0)
                vm.coresPerSocket = vmConfigInfo?.data?.data?.cores?.toInteger() ?: 0

               vm.datastores = vmConfigInfo.data?.data.findAll { k, v ->
                    // Match disk keys like 'virtio0', 'scsi0', 'ide1', etc.
                    k ==~ /^(virtio|scsi|ide|sata)\d+$/ && v instanceof String && v.contains(':')
                }.collect { k, v ->
                    // Extract the storage ID before the colon
                    v.split(':')[0]
                }.unique()

                vms << vm
            }
        }
        return new ServiceResponse(success: true, data: vms)
    }


    static Map getVMConfigById(HttpApiClient client, Map authConfig, String vmId, String nodeId = "0") {

        //proxmox api limitation. If we don't have the node we need to query all
        if (nodeId == 0) {
            Map vm = listVMs(client, authConfig).data.find { it.vmid == vmId }
            if (!vm) {
                throw new Exception("Error: VM with ID $vmId not found.")
            }
            nodeId = vm.node as Long
        }
        def vmConfigInfo = callListApiV2(client, "nodes/$nodeId/qemu/$vmId/config", authConfig)

        return vmConfigInfo.data.data
    }


    static ServiceResponse listProxmoxPools(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxPools...")
        def pools = []

        List<Map> poolIds = callListApiV2(client, "pools", authConfig).data

        poolIds.each { Map pool ->
            Map poolData = callListApiV2(client, "pools/$pool.poolid", authConfig).data
            pools << poolData
        }

        return new ServiceResponse(success: true, data: pools)
    }


    static ServiceResponse getProxmoxHypervisorHostByName(HttpApiClient client, Map authConfig, String nodeId) {
        def resp = listProxmoxHypervisorHosts(client, authConfig)
        def node = resp.data.find { it.node == nodeId }

        return new ServiceResponse(success: resp.success, data: node)
    }


    static ServiceResponse getProxmoxHypervisorNodeIds(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxHosts...")

        List<String> hvHostIds = callListApiV2(client, "nodes", authConfig).data.collect { it.node as String }

        return new ServiceResponse(success: true, data: hvHostIds)
    }


    static ServiceResponse listProxmoxHypervisorHosts(HttpApiClient client, Map authConfig) {
        log.debug("listProxmoxHosts...")

        // Workaround: Get networks safely with error handling
        List<Map> allInterfaces = []
        try {
            ServiceResponse networkResponse = listProxmoxNetworks(client, authConfig)
            if (networkResponse?.success && networkResponse?.data) {
                allInterfaces = networkResponse.data
            }
        } catch (Exception e) {
            log.warn("Failed to get network interfaces, continuing without them: ${e.message}")
            allInterfaces = []
        }
        
        // Get datastores safely with error handling  
        List<Map> allDatastores = []
        try {
            ServiceResponse datastoreResponse = listProxmoxDatastores(client, authConfig)
            if (datastoreResponse?.success && datastoreResponse?.data) {
                allDatastores = datastoreResponse.data
            }
        } catch (Exception e) {
            log.warn("Failed to get datastores, continuing without them: ${e.message}")
            allDatastores = []
        }

        def nodes = callListApiV2(client, "nodes", authConfig).data
        nodes.each { Map hvHost ->
            try {
                def nodeNetworkInfo = callListApiV2(client, "nodes/$hvHost.node/network", authConfig)
                
                // Check if network info was retrieved successfully
                if (!nodeNetworkInfo.success || !nodeNetworkInfo.data) {
                    log.warn("Failed to retrieve network info for node ${hvHost.node}, setting default IP")
                    hvHost.ipAddress = "0.0.0.0"  // Set default IP for offline nodes
                } else {
                    def sortedNetworks = nodeNetworkInfo.data.sort { a, b ->
                        def aIface = a?.iface
                        def bIface = b?.iface

                        // Push null/empty iface to the bottom
                        if (!aIface && bIface) return 1
                        if (!bIface && aIface) return -1
                        if (!aIface && !bIface) return 0

                        // Prioritize vmbr0
                        if (aIface == 'vmbr0') return -1
                        if (bIface == 'vmbr0') return 1

                        // Normal alphabetical sort
                        return aIface <=> bIface
                    }
                    
                    log.debug("Sorted Networks for node ${hvHost.node}: $sortedNetworks")
                    
                    // Find the first network interface with a valid address
                    def validInterface = sortedNetworks.find { it != null && it.address != null && it.address.trim() != "" }
                    
                    if (validInterface) {
                        hvHost.ipAddress = validInterface.address
                        log.debug("Set IP address for node ${hvHost.node}: ${hvHost.ipAddress}")
                    } else {
                        log.warn("No valid network interface found for node ${hvHost.node}, using node name as fallback")
                        hvHost.ipAddress = hvHost.node  // Use node name as fallback
                    }
                }

                // Set networks (with null checking and safe fallback)
                if (allInterfaces) {
                    hvHost.networks = allInterfaces
                            ?.findAll { it?.host == hvHost.node || it?.host == 'all' }
                            ?.collect { it?.iface }
                            ?.findAll { it != null } ?: []
                } else {
                    // Fallback: extract from direct node network call
                    hvHost.networks = nodeNetworkInfo?.data?.findAll { it?.iface }?.collect { it.iface } ?: []
                }

                // Set datastores (with null checking and safe fallback)
                if (allDatastores) {
                    hvHost.datastores = allDatastores
                            ?.findAll { ds -> 
                                def dsNodes = ds?.nodes?.toString()
                                return dsNodes && (dsNodes.split(",").contains(hvHost.node) || dsNodes == 'all')
                            }
                            ?.collect { it?.storage }
                            ?.findAll { it != null } ?: []
                } else {
                    // Fallback: try to get datastores directly for this node
                    try {
                        ServiceResponse nodeStorageResponse = callListApiV2(client, "nodes/${hvHost.node}/storage", authConfig)
                        if (nodeStorageResponse?.success && nodeStorageResponse?.data) {
                            hvHost.datastores = nodeStorageResponse.data.collect { it?.storage }.findAll { it != null }
                        } else {
                            hvHost.datastores = []
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get storage for node ${hvHost.node}: ${e.message}")
                        hvHost.datastores = []
                    }
                }
                        
            } catch (Exception e) {
                log.error("Error processing node ${hvHost.node}: ${e.message}", e)
                // Set default values for failed nodes
                hvHost.ipAddress = "0.0.0.0"
                hvHost.networks = []
                hvHost.datastores = []
            }
        }

        return new ServiceResponse(success: true, data: nodes)
    }
    
    
    private static ServiceResponse callListApiV2(HttpApiClient client, String path, Map authConfig) {
        log.debug("callListApiV2: path: ${path}")

        def tokenCfg = getApiV2Token(authConfig).data
        def rtn = new ServiceResponse(success: false)
        try {
            rtn.data = []
            def opts = new HttpApiClient.RequestOptions(
                    headers: [
                        'Content-Type': 'application/json',
                        'Cookie': "PVEAuthCookie=$tokenCfg.token",
                        'CSRFPreventionToken': tokenCfg.csrfToken
                    ],
                    contentType: ContentType.APPLICATION_JSON,
                    ignoreSSL: true
            )
            def results = client.callJsonApi(authConfig.apiUrl, "${authConfig.v2basePath}/${path}", null, null, opts, 'GET')
            def resultData = results.toMap().data.data
            log.debug("callListApiV2 results: ${resultData}")
            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                rtn.data = resultData
            } else {
                if(!rtn.success) {
                    rtn.msg = results.data + results.errors
                    rtn.success = false
                }
            }
        } catch(e) {
            log.error "Error in callListApiV2: ${e}", e
            rtn.msg = "Error in callListApiV2: ${e}"
            rtn.success = false
        }
        return rtn
    }


    private static ServiceResponse getApiV2Token(Map authConfig) {
        def path = "access/ticket"
        //log.debug("getApiV2Token: path: ${path}")
        HttpApiClient client = new HttpApiClient()

        def rtn = new ServiceResponse(success: false)
        try {

            def encUid = URLEncoder.encode((String) authConfig.username, "UTF-8")
            def encPwd = URLEncoder.encode((String) authConfig.password, "UTF-8")
            def bodyStr = "username=" + "$encUid" + "&password=$encPwd"

            HttpApiClient.RequestOptions opts = new HttpApiClient.RequestOptions(
                    headers: ['Content-Type':'application/x-www-form-urlencoded'],
                    body: bodyStr,
                    contentType: ContentType.APPLICATION_FORM_URLENCODED,
                    ignoreSSL: true
            )
            def results = client.callJsonApi(authConfig.apiUrl,"${authConfig.v2basePath}/${path}", opts, 'POST')

            //log.debug("getApiV2Token API request results: ${results.toMap()}")
            if(results?.success && !results?.hasErrors()) {
                rtn.success = true
                def tokenData = results.data.data
                rtn.data = [csrfToken: tokenData.CSRFPreventionToken, token: tokenData.ticket]

            } else {
                rtn.success = false
                rtn.msg = "Error retrieving token: $results.data"
                log.error("Error retrieving token: $results.data")
            }
            return rtn
        } catch(e) {
            log.error "Error in getApiV2Token: ${e}", e
            rtn.msg = "Error in getApiV2Token: ${e}"
            rtn.success = false
        }
        return rtn
    }
}