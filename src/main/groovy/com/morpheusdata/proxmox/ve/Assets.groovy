package com.morpheusdata.proxmox.ve

import groovy.transform.CompileStatic

@CompileStatic
enum Assets {
    PROXMOX_LOGO_STACKED('proxmox-logo-stacked-color.svg'),
    PROXMOX_LOGO_STACKED_INVERTED('proxmox-logo-stacked-inverted-color.svg'),
    PROXMOX_FULL_LOCKUP('proxmox-full-lockup-color.svg'),
    PROXMOX_FULL_LOCKUP_INVERTED('proxmox-full-lockup-inverted-color.svg'),
    MORPHEUS_LOGO('morpheus.svg'),
    ;

    private final String path

    Assets(final String path) {
        this.path = path
    }

    String getPath() {
        return this.path
    }
}
