/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.event;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.NotNull;

public class QueryMonitorConfig
{
    private DataSize maxOutputStageJsonSize = new DataSize(16, Unit.MEGABYTE);

    private String userName;
    private String password;
    private String url;
    private int maxActive = 5;
    private int maxWait = 60000;
    private boolean poolMonitor = false;
    private boolean enableMonitor = false;

    @MinDataSize("1kB")
    @MaxDataSize("1GB")
    @NotNull
    public DataSize getMaxOutputStageJsonSize()
    {
        return maxOutputStageJsonSize;
    }

    @Config("event.max-output-stage-size")
    public QueryMonitorConfig setMaxOutputStageJsonSize(DataSize maxOutputStageJsonSize)
    {
        this.maxOutputStageJsonSize = maxOutputStageJsonSize;
        return this;
    }

    public String getUserName()
    {
        return userName;
    }

    @Config("querylog.jdbc.username")
    @ConfigDescription("username of mysql")
    public QueryMonitorConfig setUserName(String userName)
    {
        this.userName = userName;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("querylog.jdbc.password")
    @ConfigDescription("password of mysql")
    public QueryMonitorConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getUrl()
    {
        return url;
    }

    @Config("querylog.jdbc.url")
    @ConfigDescription("URL of mysql")
    public QueryMonitorConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    public int getMaxActive() {
        return maxActive;
    }

    @Config("querylog.jdbc.max-active")
    public QueryMonitorConfig setMaxActive(int maxActive) {
        this.maxActive = maxActive;
        return this;
    }

    public int getMaxWait() {
        return maxWait;
    }

    @Config("querylog.jdbc.max-wait")
    public QueryMonitorConfig setMaxWait(int maxWait) {
        this.maxWait = maxWait;
        return this;
    }


    public boolean isPoolMonitor() {
        return poolMonitor;
    }

    @Config("querylog.jdbc.pool-monitor")
    public QueryMonitorConfig setPoolMonitor(boolean poolMonitor) {
        this.poolMonitor = poolMonitor;
        return this;
    }

    public boolean isEnableMonitor() {
        return enableMonitor;
    }

    @Config("querylog.jdbc.enable-monitor")
    public QueryMonitorConfig setEnableMonitor(boolean enableMonitor) {
        this.enableMonitor = enableMonitor;
        return this;
    }
}
