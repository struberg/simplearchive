/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apacheextras.simplearchive.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apacheextras.simplearchive.impl.SimpleArchiveStorage;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:struberg@yahoo.de">Mark Struberg</a>
 */
public class SimpleArchiveStorageTest extends ArchiveTestBase
{

    @Test
    public void testSimpleArchiveWriteDocument() throws IOException, InterruptedException
    {
        SimpleArchiveStorage sas = new SimpleArchiveStorage(tempDir.toFile().getAbsolutePath());

        try
        {
            sas.open();

            final String myDocId1 = "doc0001";
            Assert.assertFalse(sas.documentExists(myDocId1));

            Map<String, String> metadata = new HashMap<String, String>();
            metadata.put("key1", "val1");
            metadata.put("key2", "val2");
            metadata.put("key##3", "val##3");

            byte[] content = "myDocument".getBytes();

            sas.writeDocument(myDocId1, content, metadata);

            Assert.assertNotNull(sas.readDocument(myDocId1));
            Assert.assertEquals(sas.readDocument(myDocId1), content);

            Map<String, String> metadataFromArchive = sas.readMetadata(myDocId1);
            Assert.assertNotNull(metadataFromArchive);
            Assert.assertEquals(metadataFromArchive.size(), 3);


            Map<String, String> requiredMetadata = new HashMap<String, String>();
            requiredMetadata.put("key2", "val2");
            List<String> foundDocIds = sas.searchDocuments(requiredMetadata);
            Assert.assertNotNull(foundDocIds);
            Assert.assertEquals(foundDocIds.size(), 1);
            Assert.assertEquals(foundDocIds.get(0), myDocId1);
        }
        finally
        {
            sas.close();
        }
    }
}
