/** 
 
Copyright 2013 Intel Corporation, All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
*/ 

package com.intel.cosbench.config.common;

import org.apache.commons.configuration.Configuration;

import com.intel.cosbench.config.*;

/**
 * One COSBench implementation of Config interface, which encapsulates differences of different configuration format.
 * 
 * @author ywang19, qzheng7
 *
 */
class SyncConfigApator implements Config {

    private String config;

	public String getConfig() {
		return config;
	}

	public void setConfig(String config) {
		this.config = config;
	}

	@Override
	public String get(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String get(String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getInt(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(String key, int value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String key, long value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String key, double value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getBoolean(String key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBoolean(String key, boolean value) {
		// TODO Auto-generated method stub
		return false;
	}

    

}
