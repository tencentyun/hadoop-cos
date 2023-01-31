package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.model.PartSummary;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.NativeFileSystemStore;

import java.util.List;

/**
 * <p>
 * Holds information of one upload id listing for part summary
 * {@link NativeFileSystemStore}.
 * This includes the {@link PartSummary part summary}
 * (their names) contained in single MPU.
 * </p>
 *
 * @see NativeFileSystemStore#listParts(String, String)
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNPartListing {
    private final List<PartSummary> partSummaries;

    public CosNPartListing(List<PartSummary> partSummaries) {
        this.partSummaries = partSummaries;
    }

    public List<PartSummary> getPartSummaries() {
        return this.partSummaries;
    }
}
