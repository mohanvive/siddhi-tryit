/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.sample;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;

/**
 * The sample demonstrate how to use Siddhi within another Java program.
 */
public class PatternSample {

    public static void main(String[] args) throws InterruptedException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Siddhi Application
        String siddhiApp = "" +
                "define stream transactionStream (cardNo long, amount double, location string); " +
                "" +
                "@info(name = 'query1') " +
                "from every (e1 = transactionStream -> e2 = transactionStream[e1.cardNo == cardNo and amount > e1.amount]" +
                "                   -> e3 = transactionStream[e2.cardNo == cardNo and amount > e2.amount]) " +
                " within 10 sec " +
                "select e1.cardNo , e3.amount as finalTransactionAmount,  e3.location as finalLocation " +
                "insert into possibleFraudStream;";

        //Generate runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //Adding callback to retrieve output events from stream
        siddhiAppRuntime.addCallback("possibleFraudStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                //To convert and print event as a map
                //EventPrinter.print(toMap(events));
            }
        });

        //Get InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("transactionStream");

        //Start processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{111111L, 100D, "Colombo - 02"});
        inputHandler.send(new Object[]{111111L, 1000D, "Colombo - 03"});
        Thread.sleep(6000);
        inputHandler.send(new Object[]{111111L, 5000D, "Colombo - 03"});



        Thread.sleep(5000);

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();

    }
}
