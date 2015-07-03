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

import java.util.HashMap;
import java.util.Map;

import org.apacheextras.simplearchive.SimpleArchive;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:struberg@yahoo.de">Mark Struberg</a>
 */
public class SimpleArchiveTest extends ArchiveTestBase {
    public final int NUM_DOCS = 200;

    @Test
    public void textSimpleArchivePerformance() {
        SimpleArchive simpleArchive = new SimpleArchive(tempDir.toFile().getAbsolutePath());

        Map<String, String> documentAttribs = new HashMap<String, String>();
        for (int a = 0; a < 10; a++ ) {
            documentAttribs.put("key_" + a, "value_" + a);
        }

        final byte[] content = "This is a neat sample content".getBytes();

        for(int i = 0; i < NUM_DOCS; i++) {
            simpleArchive.addDocument("perfTestDocId_" + i, content, documentAttribs);
        }

        for(int i = 0; i < NUM_DOCS; i++) {
            Map<String, String> metadata = simpleArchive.readMetadata("perfTestDocId_" + i);
            Assert.assertNotNull(metadata);
            Assert.assertEquals(metadata.size(), 10);
        }
        for(int i = 0; i < 10; i++) {
            Map<String, String> searchCrit = new HashMap<>();
            searchCrit.put("key_" + i, "value_" + i );
            String[] foundDocIds = simpleArchive.searchDocuments(searchCrit);
            Assert.assertNotNull(foundDocIds);
            Assert.assertEquals(foundDocIds.length, NUM_DOCS);
        }

    }

}
