/*******************************************************************************
 * Copyright (c) 2013 Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 * Leonardo Aniello, Roberto Baldoni, Leonardo Querzoni
 *******************************************************************************/
package edu.fudan.storm.LoadMonitor;

import edu.fudan.storm.config.MonitorConfiguration;


public class TaskMonitor {

    private final int taskId;

    private long threadId;

    /**
     * in ms
     */
    private int slotLength;

    private long lastCheck;


    public TaskMonitor(int taskId) {
        this.taskId = taskId;
        threadId = -1;
        slotLength = MonitorConfiguration.getInstance().getTimeWindowSlotLength() * 1000;
    }

    public void checkThreadId() {
        if (threadId == -1) {
            threadId = Thread.currentThread().getId();
            WorkerMonitor.getInstance().registerTask(this);
        }
    }

    public int getTaskId() {
        return taskId;
    }

    public long getThreadId() {
        return threadId;
    }

}
