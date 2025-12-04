package com.morpheusdata.proxmox.ve

import com.morpheusdata.core.Plugin
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.AccountCredential
import com.morpheusdata.proxmox.ve.ProxmoxVeNodeOptionSourceProvider
import groovy.util.logging.Slf4j

/**
 * @author Neil van Rensburg
 */

@Slf4j
class ProxmoxVePlugin extends Plugin {
    
    private String networkProviderCode
    public static String V2_BASE_PATH = '/api2/json'

    @Override
    String getCode() {
        return 'proxmox-ve'
    }

    @Override
    void initialize() {
        this.setName("Proxmox VE")
        this.registerProvider(new ProxmoxVeCloudProvider(this, this.morpheus))
        this.registerProvider(new ProxmoxVeProvisionProvider(this, this.morpheus))
        this.registerProvider(new ProxmoxVeOptionSourceProvider(this, this.morpheus))
        this.registerProvider(new ProxmoxVeNodeOptionSourceProvider(this, this.morpheus))
        this.registerProvider(new ProxmoxVeVirtualImageDatasetProvider(this, this.morpheus))
        def networkProvider = new ProxmoxNetworkProvider(this, this.morpheus)
        this.registerProvider(networkProvider)
        networkProviderCode = networkProvider.code
    }
    
    def ProxmoxNetworkProvider getNetworkProvider() {
        this.getProviderByCode(networkProviderCode)
    }


    def getAuthConfig(Cloud cloud) {
        log.debug "getAuthConfig: ${cloud}"
        def rtn = [
                apiUrl    : cloud.serviceUrl,
                v2basePath: V2_BASE_PATH,
                username  : null,
                password  : null
        ]

        if(!cloud.accountCredentialLoaded) {
            AccountCredential accountCredential
            try {
                accountCredential = this.morpheus.async.cloud.loadCredentials(cloud.id).blockingGet()
            } catch(e) {
               log.error("Error loading cloud credentials: ${e}")
            }
            cloud.accountCredentialLoaded = true
            cloud.accountCredentialData = accountCredential?.data
        }

        if(cloud.accountCredentialData && cloud.accountCredentialData.containsKey('username')) {
            rtn.username = cloud.accountCredentialData['username']
        } else {
            rtn.username = cloud.configMap.username ?: cloud.serviceUsername
        }

        if(cloud.accountCredentialData && cloud.accountCredentialData.containsKey('password')) {
            rtn.password = cloud.accountCredentialData['password']
        } else {
            rtn.password = cloud.configMap.password ?: cloud.servicePassword
        }
        return rtn
    }


    /**
     * Called when a plugin is being removed from the plugin manager (aka Uninstalled)
     */
    @Override
    void onDestroy() {
        //nothing to do for now
    }
}
