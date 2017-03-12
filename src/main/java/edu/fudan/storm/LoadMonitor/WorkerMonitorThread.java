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

public class WorkerMonitorThread extends Thread {

    public void run() {

        while (true) {
            try {
                Thread.sleep(MonitorConfiguration.getInstance().getTimeWindowSlotLength() * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                WorkerMonitor.getInstance().sampleStats();
                WorkerMonitor.getInstance().storeStats();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
