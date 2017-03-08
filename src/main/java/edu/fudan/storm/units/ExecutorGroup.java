package edu.fudan.storm.units;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ExecutorGroup {

    private String topologyId;
    private final int groupID;
    private Map<Integer, Executor> executorMap; // executor.hashcode() -> executor
    private long load;

    public ExecutorGroup(int groupID, String topologyId) {
        this.groupID = groupID;
        this.topologyId=topologyId;
        executorMap = new HashMap<Integer, Executor>();
    }

    /**
     * assigns the executor to this slot and update inter-slot traffic stats
     *
     * @param executor
     */
    public void assign(Executor executor) {
        if (contains(executor))
            throw new RuntimeException("Executor " + executor + " already assigned to slot " + this);
        executorMap.put(executor.hashCode(), executor);
        load += executor.getLoad();
    }

    public boolean contains(Executor executor) {
        return executorMap.containsKey(executor.hashCode());
    }

    public long getLoad() {
        return load;
    }

    public Collection<Executor> getExecutors() {
        return executorMap.values();
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ExecutorGroup other = (ExecutorGroup) obj;
        if (groupID != other.groupID)
            return false;
        if (topologyId == null) {
            if (other.topologyId != null)
                return false;
        } else if (!topologyId.equals(other.topologyId))
            return false;
        return true;
    }
}
