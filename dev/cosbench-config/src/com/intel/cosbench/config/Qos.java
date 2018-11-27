package com.intel.cosbench.config;

import org.apache.commons.lang.StringUtils;

public class Qos {
    private String type;
    private String config;

    public Qos() {
        /* empty */
    }

    public Qos(String type) {
        setType(type);
    }

    public Qos(String type, String config) {
        setType(type);
        setConfig(config);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        if (StringUtils.isEmpty(type))
            throw new ConfigException("storage type cannot be empty");
        this.type = type;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        /* configuration might be empty */
        this.config = config;
    }

    public void validate() {
        setType(getType());
    }
}
