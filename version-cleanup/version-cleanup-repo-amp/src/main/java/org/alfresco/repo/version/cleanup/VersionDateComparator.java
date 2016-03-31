package org.alfresco.repo.version.cleanup;

import java.util.Comparator;
import java.util.Date;

import org.alfresco.service.cmr.version.Version;

/**
 * A comparator to sort a version list according version date ascending
 *
 * @author Philippe Dubois
 */
public class VersionDateComparator implements Comparator
{

    public int compare(Object version1, Object version2)
    {
        Date date1 = ((Version) version1).getFrozenModifiedDate();
        Date date2 = ((Version) version2).getFrozenModifiedDate();

        // sort the list ascending
        return date2.compareTo(date1);
    }
}
