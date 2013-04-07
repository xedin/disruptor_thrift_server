/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.tinkerpop.thrift;

import java.util.Random;

import com.tinkerpop.thrift.test.TestService;
import org.apache.thrift.transport.TTransport;

import org.junit.Test;

public class MultiRequestTest extends AbstractDisruptorTest
{
    private static final int REQUESTS = 500;

    @Test
    public void multiRequestTest() throws Exception
    {
        Random random = new Random();
        TTransport transport = getNewTransport();

        try
        {
            TestService.Client client = getNewClient(transport);

            for (int i = 0; i < REQUESTS; i += 4)
                invokeRequests(client, i, random.nextInt(5000), random.nextInt(50000));
        }
        finally
        {
            transport.close();
        }
    }
}
