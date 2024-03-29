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
package org.apacheextras.simplearchive.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Access the metadata and the archive storage.
 * This class is stateful. The {@link #open()} method retains a lock on the metadata file
 * and {@link #close()} will unlock again.
 *
 * This class is NOT synchronized!
 *
 * <h3>The format of the metadata file looks as following.
 * The first line contains a 'version' number which gets incremented with every write.
 * For content the '##' token is used as separator.</h3>
 * <pre>
 * {documentId}##documentId##{filename}
 * {documentId}##{attributeName}###{attributeValue}
 * </pre>
 *
 * An example would be:
 * <pre>
 * 1438
 * zep00033320012##documentId##zep00033320012.pdf
 * zep00033320012##name##sales_contract
 * zep00033320012##assignedTo##karl@somecompany.sample
 * zep0033200848#documentId##zep0033200848.pdf
 * zep0033200848##name##offer_doc
 * zep0033200848##assignedTo##none
 * </pre>
 *
 * @author <a href="mailto:struberg@yahoo.de">Mark Struberg</a>
 */
public class SimpleArchiveStorage
{
    private static final String METADATA_FILE_NAME = "simplearchive.metadata";

    /** The content itself might actually not be a pdf. This is just used for simplified browsing */
    private static final String DOCUMENT_FILE_EXTENSION = ".pdf";

    private static final String METADATA_SEPARATOR = "##";
    private static final String METADATA_DOCNAME = "documentFileName";

    private final String storageLocation;

    private boolean opened = false;
    private RandomAccessFile metadataAccess;
    private FileLock metadataLock;

    /**
     * We now simply cache the full metadata attributes;
     * Note that this might get big! Well, it's only a SIMPLE archive intended for testing only ;)
     */
    private List<String> contentLines = new ArrayList<>();

    /**
     * We also implement optimistic locking.
     * Means the {@link #contentLines} only will get refreshed when the version in the file
     * got changed by another thread/JVM.
     */
    private AtomicInteger optLock = new AtomicInteger(0);


    public SimpleArchiveStorage(String storageLocation)
    {
        this.storageLocation = storageLocation;

            File storage = new File(storageLocation);
            if (!storage.exists()) {
                storage.mkdirs();
            }
    }

    public void open() throws InterruptedException, IOException
    {
        if (opened)
        {
            throw new IllegalStateException("Archive is already opened");
        }

        RandomAccessFile metadataStore = getMetadataAccess();
        metadataLock = metadataStore.getChannel().lock();
        opened = true;
    }


    public void close()
    {
        if (!opened)
        {
            return;
        }

        try {
            metadataLock.close();
            metadataAccess.close();
            metadataLock = null;
            metadataAccess = null;
            opened = false;
        }
        catch (IOException ioe)
        {
            throw new RuntimeException(ioe);
        }
    }

    public boolean isOpen()
    {
        return opened;
    }


    public boolean documentExists(String documentId)
    {
        File documentFile = new File(storageLocation, documentId + DOCUMENT_FILE_EXTENSION);
        return documentFile.exists();
    }

    /**
     *
     * @param documentId
     * @param document only gets written if not null
     * @param documentMetadata
     * @throws IOException
     */
    public void writeDocument(String documentId, byte[] document, Map<String, String> documentMetadata) throws IOException
    {
        if (document != null)
        {
            File binFile = new File(storageLocation, documentId + DOCUMENT_FILE_EXTENSION);
            if (binFile.exists())
            {
                binFile.delete();
            }
            FileOutputStream fos = new FileOutputStream(binFile);
            fos.write(document);
            fos.flush();
            fos.close();
        }

        if (documentMetadata != null) {
            List<String> content = readAllLines(metadataAccess);
            content = setMetadata(content, documentId, documentMetadata);
            writeAllLines(metadataAccess, content);
        }
    }


    public List<String> searchDocuments(Map<String, String> requiredDocumentMetadata) throws IOException
    {
        // structure is as following
        // docid -> found criterias
        Map<String, Set<String>> matchDocs = new HashMap<String, Set<String>>();

        List<String> lines = readAllLines(metadataAccess);
        for (String line : lines)
        {
            String[] parts = line.split("\\#\\#");
            if (parts[1].equals(METADATA_DOCNAME))
            {
                continue;
            }
            String key = deEscape(parts[1]);
            String value = deEscape(parts[2]);

            for (Map.Entry<String, String> metadataEntry : requiredDocumentMetadata.entrySet())
            {
                if (key.equals(metadataEntry.getKey()) &&
                    (value.endsWith("%")
                            ? value.startsWith(metadataEntry.getValue().substring(metadataEntry.getValue().length()-1))
                            : value.equals(metadataEntry.getValue())))
                {
                    String documentId = deEscape(parts[0]);
                    Set<String> foundAttributes = matchDocs.get(documentId);
                    if (foundAttributes == null)
                    {
                        foundAttributes = new HashSet<String>();
                        matchDocs.put(documentId, foundAttributes);
                    }
                    foundAttributes.add(metadataEntry.getValue());
                }
            }
        }

        List<String> foundDocs = new ArrayList<String>();
        //X TODO fuellen
        int argumentCount = requiredDocumentMetadata.size();
        for (Map.Entry<String, Set<String>> docEntry : matchDocs.entrySet())
        {
            if (docEntry.getValue().size() == argumentCount)
            {
                foundDocs.add(docEntry.getKey());
            }
        }

        return foundDocs;
    }


    public byte[] readDocument(String documentId) throws IOException
    {
        File documentFile = new File(storageLocation, documentId + DOCUMENT_FILE_EXTENSION);
        if (!documentFile.exists())
        {
            return null;
        }
        Path path = Paths.get(documentFile.toURI());
        return Files.readAllBytes(path);
    }


    public Map<String, String> readMetadata(String documentId) throws IOException
    {
        Map<String, String> metadata = new HashMap<>();
        List<String> content = readAllLines(metadataAccess);
        final String metadataStart = escape(documentId) + METADATA_SEPARATOR;
        Iterator<String> contentIt = content.iterator();
        while (contentIt.hasNext())
        {
            String line = contentIt.next();
            if (line.startsWith(metadataStart))
            {
                String[] parts = line.split("\\#\\#");
                if (parts[1].equals(METADATA_DOCNAME))
                {
                    continue;
                }
                metadata.put(deEscape(parts[1]), deEscape(parts[2]));
            }
        }

        return metadata;
    }

    public void deleteDocument(String documentId) throws IOException
    {
        File documentFile = new File(storageLocation, documentId + DOCUMENT_FILE_EXTENSION);
        documentFile.delete();

        List<String> content = readAllLines(metadataAccess);
        content = removeMetadata(content, documentId);
        writeAllLines(metadataAccess, content);
    }


    private RandomAccessFile getMetadataAccess() throws IOException
    {
        if (metadataAccess == null)
        {
            File metadataFile = new File(storageLocation, METADATA_FILE_NAME);
            metadataFile.createNewFile();
            metadataAccess = new RandomAccessFile(metadataFile, "rw");
        }

        return metadataAccess;
    }

    /**
     * For simplicity reason we just always read and replace the whole metadata file content.
     * This implementation is NOT tuned for performance as you see ;)
     * @return all the content of the file
     */
    private List<String> readAllLines(RandomAccessFile file) throws IOException
    {
        List<String> content = new ArrayList<String>();
        file.seek(0);

        String line;

        // first line is the version
        line = file.readLine();
        if (line == null || line.length() == 0)
        {
            // an empty archive
            optLock.set(0);
            return content;
        }
        else if (line.contains("##"))
        {
            // old archive file, implicitly convert it
            content.add(line);
            optLock.set(0);
        }
        else {
            int version = Integer.parseInt(line);
            if (optLock.get() == version) {
                // file did not get changed in the meantime -> fine with the current content
                return contentLines;
            }
        }

        while ((line = file.readLine()) != null)
        {
            if (line.trim().length() > 0)
            {
                content.add(line);
            }
        }

        return content;
    }

    private void writeAllLines(RandomAccessFile file, List<String> content) throws IOException
    {
        // we give a damn about memory and write all at once - remember it's a SIMPLE archive...
        StringBuffer sb = new StringBuffer(2^18);

        for (String line : content)
        {
            sb.append(line).append('\n');
        }

        file.seek(0);

        // handle optimistic locking
        int newVersion = optLock.incrementAndGet();
        file.writeBytes(Integer.toString(newVersion));
        file.write('\n');

        // and update the cached content
        contentLines = content;

        // and now write the content all in one go
        file.writeBytes(sb.toString());
    }

    private List<String> setMetadata(List<String> content, String documentId, Map<String, String> documentMetadata)
    {
        content = removeMetadata(content, documentId);

        // now add the new content
        String escapedDocumentId = escape(documentId);

        // document id line
        content.add(escapedDocumentId + "##" + METADATA_DOCNAME + "##" + escapedDocumentId + DOCUMENT_FILE_EXTENSION);

        // now all attributes
        for (Map.Entry<String, String> metadataEntry : documentMetadata.entrySet())
        {
            content.add(escapedDocumentId + "##" + escape(metadataEntry.getKey()) + "##" + escape(metadataEntry.getValue()));
        }
        return content;
    }

    private List<String> removeMetadata(List<String> content, String documentId)
    {
        final String metadataStart = escape(documentId) + METADATA_SEPARATOR;
        Iterator<String> contentIt = content.iterator();
        while (contentIt.hasNext())
        {
            String line = contentIt.next();
            if (line.startsWith(metadataStart))
            {
                contentIt.remove();
            }
        }
        return content;
    }


    //X TODO probably also escape line feeds?
    private String escape(String attribute)
    {
        return attribute.replace("##", "\\#\\#");
    }

    private String deEscape(String attribute)
    {
        return attribute.replace("\\#\\#", "##");
    }

}
