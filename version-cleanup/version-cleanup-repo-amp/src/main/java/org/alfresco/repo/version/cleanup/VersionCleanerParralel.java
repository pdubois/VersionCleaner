/*
 * Copyright (C) 2005-2007 Alfresco Software Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

 * As a special exception to the terms and conditions of version 2.0 of 
 * the GPL, you may redistribute this Program in connection with Free/Libre 
 * and Open Source Software ("FLOSS") applications as described in Alfresco's 
 * FLOSS exception.  You should have recieved a copy of the text describing 
 * the FLOSS exception, and it is also available here: 
 * http://www.alfresco.com/legal/licensing
 */
package org.alfresco.repo.version.cleanup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.alfresco.error.AlfrescoRuntimeException;
import org.alfresco.repo.transaction.RetryingTransactionHelper.RetryingTransactionCallback;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.cmr.rule.RuleService;
import org.alfresco.service.cmr.search.ResultSet;
import org.alfresco.service.cmr.search.SearchParameters;
import org.alfresco.service.cmr.search.SearchService;
import org.alfresco.service.cmr.version.Version;
import org.alfresco.service.cmr.version.VersionHistory;
import org.alfresco.service.cmr.version.VersionService;
import org.alfresco.service.descriptor.DescriptorService;
import org.alfresco.service.namespace.NamespaceService;
import org.alfresco.service.namespace.QName;
import org.alfresco.service.transaction.TransactionService;
import org.alfresco.util.Pair;
import org.alfresco.util.PropertyCheck;
import org.alfresco.util.VmShutdownListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.alfresco.repo.batch.BatchProcessor;
import org.alfresco.repo.batch.BatchProcessor.BatchProcessWorker;
import org.alfresco.repo.lock.JobLockService;
import org.alfresco.repo.security.authentication.*;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

/**
 * This component is responsible for finding old versions and deleting them. Deletion handlers can be provided to ensure
 * that the version is moved to another location prior to being deleted
 * 
 * @author Derek Hulley
 * @author Philippe Dubois
 */
public class VersionCleanerParralel implements ApplicationEventPublisherAware
{
    private static Log logger = LogFactory.getLog(VersionCleanerParralel.class);
    private static final QName LOCK_QNAME = QName.createQName(NamespaceService.SYSTEM_MODEL_1_0_URI,
            "VersionStoreCleaner");
    private static ThreadLocal<Pair<Long, String>> lockThreadLocal = new ThreadLocal<Pair<Long, String>>();
    //keep lock 2 Hours
    private static final long LOCK_TTL = 7200000L;
    /** kept to notify the thread that it should quit */
    private static VmShutdownListener vmShutdownListener = new VmShutdownListener("VersionStoreCleaner");

    private static final Comparator VERSION_DATE_COMPARATOR = new VersionDateComparator();
    private static final Comparator VERSION_DATE_COMPARATOR_REVERSE = new VersionDateComparatorReverse();
    //private static final String VERSION_SEARCH_STRING = "+ASPECT:\"{http://www.alfresco.org/model/content/1.0}versionable\"" +
    //                                                     "  -ASPECT:\"{http://www.alfresco.org/model/content/1.0}workingcopy\"";
    private static final String VERSION_SEARCH_STRING = "ASPECT:\"{http://www.alfresco.org/model/content/1.0}versionable\"";
    private int bigPageLen = 50000;
    private TransactionService transactionService;
    private VersionService versionService;
    private SearchService searchService;
    private NodeService nodeService;
    private JobLockService jobLockService;
    private DescriptorService descriptorService;
    private ApplicationEventPublisher applicationEventPublisher;
    private List<VersionStoreCleanerListener> listeners;
    private int maxVersionsToKeep = 50;
    private int minVersionsToKeep = 10;
    private int maxDaysToKeep = 365 * 15; // Approx 15 years by default
    private int threadNumber = 2;
    private String searchLimiter;
    private Date startDate;
    private Date endDate;
    private Boolean isRunning;
    /**
     * Running end date of the version cleaning process
     */
    public Date getEndDate()
    {
        return endDate;
    }

    /**
     * Running end date of the version cleaning process
     */
    public Date getStartDate()
    {
        return startDate;
    }


    /**
     * Is the process running
     */
    public Boolean getIsRunning()
    {
        return isRunning;
    }

    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher)
    {
        this.applicationEventPublisher = applicationEventPublisher;
    }


    public void setNodeService(NodeService nodeService)
    {
        this.nodeService = nodeService;
    }

    /**
     * @param Return the number of versions keps
     */
    public int getMinVersionsToKeep()
    {
        return minVersionsToKeep;
    }

    /**
     * @param Return the maximum number of days that a version is keps
     */
    public int getMaxDaysToKeep()
    {
        return maxDaysToKeep;
    }
    
    /**
     * @param Set the number of threads used by the cleaning process 
     */
    public void setThreadNumber(int threadNumber)
    {
        this.threadNumber = threadNumber;
    }
    
    
    /**
     * Not all the nodeRef of the node having to be checked are give to the 
     * BatchProcessor because it would be necessary to load all the nodeRefs in 
     * memory. Paginate calls to BatchProcessor.
     * @param bigPageLen nodes are given to BatchProcessor per iteration. 
     */
    public void setBigPageLen(int bigPageLen)
    {
        this.bigPageLen = bigPageLen;
    }

    /**
     * @param jobLockService service used to ensure that cleanup runs are not duplicated
     */
    public void setJobLockService(JobLockService jobLockService)
    {
        this.jobLockService = jobLockService;
    }

    public VersionCleanerParralel()
    {
        this.listeners = new ArrayList<VersionStoreCleanerListener>(0);
    }

    /**
     * @param descriptorService used to determine the current Alfresco version
     */
    public void setDescriptorService(DescriptorService descriptorService)
    {
        this.descriptorService = descriptorService;
    }

    /**
     * @param versionService used to delete older versions
     */
    public void setVersionService(VersionService versionService)
    {
        this.versionService = versionService;
    }

    /**
     * @param queryService used to retrieve older versions
     */
    public void setSearchService(SearchService searchService)
    {
        this.searchService = searchService;
    }

    /**
     * @param transactionService the component to ensure proper transactional wrapping
     */
    public void setTransactionService(TransactionService transactionService)
    {
        this.transactionService = transactionService;
    }

    /**
     * @param listeners the listeners that can react to deletions
     */
    public void setListeners(List<VersionStoreCleanerListener> listeners)
    {
        this.listeners = listeners;
    }

    /**
     * Set the age in days that determines which of the versions that lie between the minVersionsToKeep and
     * maxVersionsToKeep are deleted. A Version will be deleted if: ( It is older than the maxVersionsToKeep'th version
     * ) OR ( (it is older than the minVersionsToKeep'th version) AND (it is older than maxDaysToKep) ) Default is 10
     * 
     * @param maxDaysToKeep Age cutoff (in days) for versions to keep.
     */
    public void setMaxDaysToKeep(int maxDaysToKeep)
    {
        this.maxDaysToKeep = maxDaysToKeep;
    }

    /**
     * Set the maximum numbers of versions to keep. All versions older than this version will be deleted. This must be
     * greater than minVersionsToKeep Default is 10
     * 
     * @param maxVersionsToKeep Cutoff (in number of versions ) for versions to keep
     */
    public void setMaxVersionsToKeep(int maxVersionsToKeep)
    {
        this.maxVersionsToKeep = maxVersionsToKeep;
    }

    /**
     * Return the maximum number of version that will be kept
     */
    public int getMaxVersionsToKeep()
    {
        return this.maxVersionsToKeep;
    }

    /**
     * Set the minimum numbers of versions to keep. No versions younger than this version will be deleted. This must be
     * smaller than maxVersionsToKeep Default is 2
     * 
     * @param minVersionsToKeep Cutoff (in number of versions ) for versions to delete
     */
    public void setMinVersionsToKeep(int minVersionsToKeep)
    {
        this.minVersionsToKeep = minVersionsToKeep;
    }

    /**
     * Limit the searches for versionable nodes. This string is appended to VERSION_SEARCH_STRING to return the nodes to
     * check for histories
     * 
     * @param searchLimiter
     */
    public void setSearchLimiter(String searchLimiter)
    {
        this.searchLimiter = searchLimiter;
    }

    /**
     * Perform basic checks to ensure that the necessary dependencies were injected.
     */
    private void checkProperties()
    {
        PropertyCheck.mandatory(this, "versionService", versionService);
        PropertyCheck.mandatory(this, "transactionService", transactionService);
        PropertyCheck.mandatory(this, "searchService", searchService);

        if (Integer.parseInt(descriptorService.getCurrentRepositoryDescriptor().getVersionMajor()) < 3)
        {
            if ((minVersionsToKeep == 0) && (maxVersionsToKeep == 0))
            {
                logger
                        .warn("minVersionsToKeep and maxVersionsToKeep are both set to zero. All found version histories will be deleted.");
            }
            else
            {
                throw new AlfrescoRuntimeException(
                        "Deleting partial version histories is not supported prior to Alfresco version 3");
            }
            return;
        }

        // check the protect days
        if (maxDaysToKeep <= 0)
        {
            logger.warn("Property 'maxDaysToKeep' is set to 0.");
        }
        else
        {
            logger.info("maxDaysToKeep set to " + maxDaysToKeep);
        }

        // check maxVersionsToKeep and minVersionsToKeep
        if ((minVersionsToKeep < 0) || (maxVersionsToKeep < 0) || (minVersionsToKeep > maxVersionsToKeep))
        {
            String message = "";
            if (maxVersionsToKeep < 0)
            {
                message = "Property 'maxVersionsToKeep' must be 0 or greater (0 is not recommended)";
            }
            else if (maxVersionsToKeep < 0)
            {
                message = "Property 'minVersionsToKeep' must be 0 or greater (0 is not recommended)";
            }
            else
            {
                message = "Property 'minVersionsToKeep' must be less than 'maxVersionsToKeep'";
            }
            throw new AlfrescoRuntimeException(message);

        }
        else
        {
            if ((maxVersionsToKeep == 0) && (minVersionsToKeep == 0))
            {
                logger
                        .warn("Property 'maxVersionsToKeep' and 'minVersionsToKeep' are set to 0. All versions will be deleted.");
            }
            else
            {
                logger.info("maxVersionsToKeep is set to " + maxVersionsToKeep);
                logger.info("minVersionsToKeep is set to " + minVersionsToKeep);
            }
        }
    }

    public void execute()
    {
        checkProperties();

        // Bypass if the system is in read-only mode
        if (transactionService.isReadOnly())
        {
            logger.debug("Version store cleaner bypassed; the system is read-only.");
            return;
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Starting version store cleanup.");
        }
        refreshLock();
        // Repeat attempts six times waiting 10 minutes between
        startDate = new Date();
        isRunning = true;
        try
        {

            executeInternal();
        }
        catch (VmShutdownException e)
        {
            // Aborted
            if (logger.isDebugEnabled())
            {
                logger.debug("   Content store cleanup aborted.");
            }
        }
        finally
        {
            endDate = new Date();
            isRunning = false;
            releaseLock();
        }

    }

    public void executeInternal()
    {
        AuthenticationUtil.setFullyAuthenticatedUser(AuthenticationUtil.getSystemUserName());
        int startingElement = 0;
        int lot = 0;
        while (true)
        {
            lot++;
            // search by page
            refreshLock();
            final int staticStartingElement = startingElement;
            // execute in READ-WRITE txn
            RetryingTransactionCallback<Collection<NodeRef>> executeCallback = new RetryingTransactionCallback<Collection<NodeRef>>()
                {
                    public Collection<NodeRef> execute() throws Exception
                    {
                        StoreRef storeRef = new StoreRef(StoreRef.PROTOCOL_WORKSPACE, "SpacesStore");
                        String limitedSearchString = VERSION_SEARCH_STRING + searchLimiter;
                        // Get VersionableNodes
                        Collection<NodeRef> versionableNodes = executeQuery(storeRef, searchService,
                                limitedSearchString, staticStartingElement, bigPageLen);
                        return versionableNodes;
                    };
                };

            try
            {
                if (vmShutdownListener.isVmShuttingDown())
                {
                    throw new VmShutdownException();
                }
                Collection<NodeRef> nodesToCleaned = transactionService.getRetryingTransactionHelper().doInTransaction(
                        executeCallback, true);
                // nbrTotOfDeletedVersions += deleteVersions(nodesToCleaned);
                // clean the versions in parralell
                //final BatchProcessor<NodeRef> groupProcessor = new BatchProcessor<NodeRef>(logger,
                //        this.transactionService.getRetryingTransactionHelper(), this.ruleService,
                //        this.applicationEventPublisher, nodesToCleaned, "VersionCleaner", 5000, threadNumber, transactionSize);
                final BatchProcessor<NodeRef> groupProcessor = new BatchProcessor<NodeRef>("VersionCleaner", this.transactionService.getRetryingTransactionHelper(), nodesToCleaned, threadNumber,
                        5000,this.applicationEventPublisher, logger, 500);
                final Date deleteOlder = new Date(System.currentTimeMillis() - (long) maxDaysToKeep * 3600L * 1000L
                        * 24L);
                class NodeVersionCleaner implements BatchProcessWorker<NodeRef>
                {
                    public String getIdentifier(NodeRef entry)
                    {
                        return entry.toString();
                    }

                    public void process(NodeRef currentNode) throws Throwable
                    {
                        AuthenticationUtil.setFullyAuthenticatedUser(AuthenticationUtil.getSystemUserName());
                        // Clean one node
                        if (!nodeService.exists(currentNode))
                            return;
                        VersionHistory history = versionService.getVersionHistory(currentNode);
                        if (history == null)
                            return;
                        List<Version> versions = new ArrayList<Version>(history.getAllVersions());
                        Collections.sort(versions, VERSION_DATE_COMPARATOR);

                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Checking versions for node " + currentNode.getId());
                        }
                        // First delete all the versions over the maxVersionsToKeep
                        for (int index = maxVersionsToKeep; index < versions.size(); index++)
                        {
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Node " + currentNode.getId() + " - Version "
                                        + versions.get(index).getVersionLabel() + " is greater than "
                                        + maxVersionsToKeep + " - deleting.");
                            }
                            for (VersionStoreCleanerListener listener : listeners)
                            {
                                listener.beforeDelete(versionService.getVersionHistory(currentNode));
                            }
                            versionService.deleteVersion(currentNode, versions.get(index));
                        }

                        // Now Check the remaining versions to see if they are old enough to get rid of
                        // but only if there are more than minVersionsToKeep left
                        // Lets be sure we keep a least minVersionsToKeep
                        if (versions.size() <= minVersionsToKeep)
                            return;

                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Checking remaining versions.");
                        }
                        
                        //****
                        history = versionService.getVersionHistory(currentNode);
                        if (history == null)
                            return;
                        versions = new ArrayList<Version>(history.getAllVersions());
                        Collections.sort(versions, VERSION_DATE_COMPARATOR);
                        //***
                        //for (int index =0 ; index < versions.size() - minVersionsToKeep; index++)
                        //for (int index = minVersionsToKeep; index < versions.size() - prevDeletedVersions; index++)
                        for (int index = minVersionsToKeep; index < versions.size() ; index++)
                        {
                            if (versions.get(index).getFrozenModifiedDate().before(deleteOlder))
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("Deleting version " + versions.get(index).getVersionLabel());
                                }
                                for (VersionStoreCleanerListener listener : listeners)
                                {
                                    listener.beforeDelete(versionService.getVersionHistory(currentNode));
                                }
                                versionService.deleteVersion(currentNode, versions.get(index));

                            }
                            else
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("Version " + versions.get(index).getVersionLabel()
                                            + " is younger than " + deleteOlder.toString() + ". No more deletions");
                                }
                            }
                        }
                    }

                    @Override
                    public void beforeProcess() throws Throwable
                    {
                        // TODO Auto-generated method stub
                        
                    }

                    @Override
                    public void afterProcess() throws Throwable
                    {
                        // TODO Auto-generated method stub
                        
                    }
                }

                NodeVersionCleaner unitOfWork = new NodeVersionCleaner();

                groupProcessor.process(unitOfWork, true);

                startingElement += bigPageLen;
                // Done
                if (logger.isDebugEnabled())
                {
                    logger.debug("  Cleaning iteration:" + staticStartingElement);
                }
                if (nodesToCleaned.size() < bigPageLen)
                    break;
            }
            catch (VmShutdownException e)
            {
                // Aborted
                if (logger.isDebugEnabled())
                {
                    logger.debug("Version cleanup aborted.");
                }
                throw e;
            }
            catch (Throwable e)
            {
                e.printStackTrace();
                logger.warn("System shutting down during version cleaning at:" + staticStartingElement);
                break;
            }
        }

    }

    private Collection<NodeRef> executeQuery(
            StoreRef storeRef,
            SearchService searchService,
            String query,
            int startingElement,
            int pageLen)
    {

        SearchParameters sp = new SearchParameters();
        sp.addStore(storeRef);
        //sp.setLanguage("lucene");
        sp.setLanguage(SearchService.LANGUAGE_FTS_ALFRESCO);
        sp.setSkipCount(startingElement);
        // -1 unlimited result size
        sp.setMaxItems(-1);
        sp.setQuery(query);
        ResultSet results = searchService.query(sp);
        Collection<NodeRef> nodeToClean = new ArrayList<NodeRef>(pageLen);
        int i;
        for (i = startingElement; i < startingElement + pageLen; i++)
        {
            if (i - startingElement >= results.length())
                break;
            NodeRef nodeRef = results.getNodeRef(i - startingElement);
            nodeToClean.add(nodeRef);
        }
        results.close();
        return nodeToClean;
    }

    /**
     * Lazily update the job lock
     */
    private void refreshLock()
    {
        Pair<Long, String> lockPair = lockThreadLocal.get();
        if (lockPair == null)
        {
            String lockToken = jobLockService.getLock(LOCK_QNAME, LOCK_TTL);
            Long lastLock = new Long(System.currentTimeMillis());
            // We have not locked before
            lockPair = new Pair<Long, String>(lastLock, lockToken);
            lockThreadLocal.set(lockPair);
        }
        else
        {
            long now = System.currentTimeMillis();
            long lastLock = lockPair.getFirst().longValue();
            String lockToken = lockPair.getSecond();
            // Only refresh the lock if we are past a threshold
            if (now - lastLock > (long) (LOCK_TTL / 2L))
            {
                jobLockService.refreshLock(lockToken, LOCK_QNAME, LOCK_TTL);
                lastLock = System.currentTimeMillis();
                lockPair = new Pair<Long, String>(lastLock, lockToken);
            }
        }
    }

    /**
     * Release the lock after the job completes
     */
    private void releaseLock()
    {
        Pair<Long, String> lockPair = lockThreadLocal.get();
        if (lockPair != null)
        {
            // We can't release without a token
            try
            {
                jobLockService.releaseLock(lockPair.getSecond(), LOCK_QNAME);
            }
            finally
            {
                // Reset
                lockThreadLocal.set(null);
            }
        }
        // else: We can't release without a token
    }

    /**
     * Message carrier to break out of loops using the callback.
     * 
     * @author Derek Hulley
     * @since 2.1.3
     */
    private class VmShutdownException extends RuntimeException
    {
        private static final long serialVersionUID = -5876107469054587072L;
    }

}
