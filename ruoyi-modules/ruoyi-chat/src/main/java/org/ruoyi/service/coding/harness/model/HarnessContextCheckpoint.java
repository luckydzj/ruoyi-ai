package org.ruoyi.service.coding.harness.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public record HarnessContextCheckpoint(
    String checkpointId,
    List<String> lineage,
    long fromSequence,
    long toSequence,
    long compactedThroughMessageSequence,
    String summary,
    List<String> artifactIds,
    long inputTokensBefore,
    long inputTokensAfter,
    String modelIdentity,
    long sourceUsageTimestamp,
    List<String> securityConstraints,
    long createdAt
) {

    public HarnessContextCheckpoint {
        lineage = lineage == null ? List.of() : List.copyOf(lineage);
        summary = summary == null ? "" : summary;
        artifactIds = artifactIds == null ? List.of() : List.copyOf(artifactIds);
        securityConstraints = securityConstraints == null
            ? List.of() : List.copyOf(securityConstraints);
        if (fromSequence < 0 || toSequence < 0 || compactedThroughMessageSequence < 0
            || inputTokensBefore < 0 || inputTokensAfter < 0 || sourceUsageTimestamp < 0
            || createdAt < 0) {
            throw new IllegalArgumentException("Invalid context checkpoint");
        }
        if (toSequence != compactedThroughMessageSequence) {
            throw new IllegalArgumentException("Checkpoint boundary fields must agree");
        }
        boolean emptyBoundary = fromSequence == 0 && toSequence == 0;
        if (!emptyBoundary && (fromSequence == 0 || fromSequence > toSequence)) {
            throw new IllegalArgumentException("Invalid checkpoint sequence range");
        }
        if (checkpointId == null || checkpointId.isBlank()) {
            if (!lineage.isEmpty()) {
                throw new IllegalArgumentException("Checkpoint lineage requires an id");
            }
            checkpointId = null;
        } else if (lineage.isEmpty() || !checkpointId.equals(lineage.get(lineage.size() - 1))) {
            throw new IllegalArgumentException("Checkpoint lineage must end at checkpoint id");
        }
        if (modelIdentity != null && modelIdentity.isBlank()) {
            modelIdentity = null;
        }
    }

    /** Compatibility constructor for snapshots written before lineage metadata was added. */
    public HarnessContextCheckpoint(long compactedThroughMessageSequence, String summary,
                                    List<String> artifactIds, long inputTokensBefore,
                                    long inputTokensAfter, long createdAt) {
        this(null, List.of(), compactedThroughMessageSequence == 0 ? 0 : 1,
            compactedThroughMessageSequence, compactedThroughMessageSequence, summary,
            artifactIds, inputTokensBefore, inputTokensAfter, null, 0, List.of(), createdAt);
    }

    public static HarnessContextCheckpoint empty() {
        return new HarnessContextCheckpoint(null, List.of(), 0, 0, 0, "",
            List.of(), 0, 0, null, 0, List.of(), 0);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return checkpointId == null && toSequence == 0 && summary.isBlank();
    }
}
