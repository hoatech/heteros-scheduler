package edu.fudan.storm.units;

import edu.fudan.storm.scheduler.ZookeeperScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.WorkerSlot;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by ybwang on 3/6/17.
 */
public class SupervisorSlot implements Comparable<SupervisorSlot>{
    public SupervisorDetails details;
    public List<WorkerSlot> avaiableSlots;
    public List<WorkerSlot> choosedSlots;
    public SupervisorSlot(SupervisorDetails details, List<WorkerSlot> slots){
        this.details=details;
        this.avaiableSlots=slots;
        choosedSlots = new LinkedList<>();
    }
    @Override
    public int compareTo(SupervisorSlot o) {
        if(o.avaiableSlots.size()<this.avaiableSlots.size())
            return 1;
        else if(o.avaiableSlots.size()>this.avaiableSlots.size())
            return -1;
        return 0;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SupervisorSlot ss = (SupervisorSlot)obj;
        if(this.details == ss.details)
            return true;
        return true;
    }
    @Override
    public String toString(){
        return "Host: "+this.details.getHost()+" available slots number: "+avaiableSlots.size()+" choosed slot number: "+choosedSlots.size();
    }

}