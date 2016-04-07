# VersionCleaner

The VersionCleaner provides a mechanism to control the number of versions kept (see VersionableAspect ).  What versions will be kept is based on 2 criteria: the age of the version and the number of versions.  Three parameters intervene: the minimum number of versions kept (minVersionsToKeep), the maximum number of versions kept (maxVersionsToKeep) and the maximum number of days that a version is kept (maxDaysToKeep). If the number of versions is bigger than maxVersionsToKeep then Version Cleaner will delete the older versions until the number of versions decreases to the upper limit (maxVersionsToKeep). Second, if the remaining versions are older than maxVersionsToKeep  then the oldest will be deleted but the number of versions kept will always be more than minVersionsToKeep. The implementation ensure that if cluster mode is used, the Version Cleaner will only start on one node at a time. The Version Cleaner will be triggered periodically. 

## Building the module


```

 mkdir work

 cd work

 git clone https://github.com/pdubois/VersionCleaner.git

 cd version-cleanup

 mvn install
 chmod +x run.sh

```

Under *version-cleanup/version-cleanup-repo-amp/target * you will find *version-cleanup-repo-amp-1.0-SNAPSHOT.amp*

## Trigger example

```

    <bean id="VersionStoreCleanerJobDetail"
          class="org.springframework.scheduling.quartz.JobDetailBean">
        <property name="jobClass">
            <value>org.alfresco.repo.version.cleanup.VersionStoreCleanupJob</value>
        </property>
        <property name="jobDataAsMap">
            <map>
                <entry key="versionStoreCleaner">
                    <ref bean="VersionStoreCleanerParralel" />
                </entry>
            </map>
        </property>
    </bean>
    
    <bean id="VersionStoreCleanerTrigger" class="org.alfresco.util.CronTriggerBean">
        <property name="jobDetail">
            <ref bean="VersionStoreCleanerJobDetail" />
        </property>
        <property name="scheduler">
            <ref bean="schedulerFactory" />
        </property>
        <property name="cronExpression">
            <value>0 0 5 * * ?</value>
        </property>
    </bean>
```



