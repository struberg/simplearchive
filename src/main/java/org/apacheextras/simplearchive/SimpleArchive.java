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
package org.apacheextras.simplearchive;

import java.util.List;
import java.util.Map;

import org.apacheextras.simplearchive.impl.SimpleArchiveStorage;

/**
 * Main Class for accessing the simple file archive.
 * The archive itself is file based and performs strict locking.
 * Thus it is possible to even use it in parallel maven builds
 * and still share the same location.
 *
 * Please note that the methods are still synchronized as nio file locks
 * only work on different JVMs.
 *
 * @author <a href="mailto:struberg@yahoo.de">Mark Struberg</a>
 */
public class SimpleArchive
{
    private final SimpleArchiveStorage sas;

    /**
     *
     * @param storageLocation the location where all the metadata and files will get stored
     */
    public SimpleArchive(String storageLocation)
    {
        try
        {
            this.sas = new SimpleArchiveStorage(storageLocation);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public synchronized void addDocument(String documentId, byte[] document, Map<String, String> documentMetadata)
    {
        try
        {
            sas.open();
            sas.writeDocument(documentId, document, documentMetadata);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            sas.close();
        }
    }

    public synchronized void setMetdata(String documentId, Map<String, String> documentMetadata)
    {
        try
        {
            sas.open();
            sas.writeDocument(documentId, null, documentMetadata);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            sas.close();
        }
    }

    /**
     * Search all the documents which have ALL the given attributes.
     * Af no attribute is given, then all documents will get returned.
     *
     * @param requiredAttributes
     * @return the documentIds of the found documents
     */
    public synchronized String[] searchDocuments(Map<String, String> requiredAttributes)
    {
        try
        {
            sas.open();
            List<String> documentIds = sas.searchDocuments(requiredAttributes);
            return documentIds.toArray(new String[documentIds.size()]);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            sas.close();
        }
    }

    public synchronized Map<String, String> readMetadata(String documentId)
    {
        try
        {
            sas.open();
            return sas.readMetadata(documentId);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            sas.close();
        }
    }

    public synchronized byte[] readDocument(String documentId)
    {
        try
        {
            sas.open();
            return sas.readDocument(documentId);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            sas.close();
        }
    }

    public synchronized void deleteDocument(String documentId)
    {
        try
        {
            sas.open();
            sas.deleteDocument(documentId);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            sas.close();
        }
    }


}
