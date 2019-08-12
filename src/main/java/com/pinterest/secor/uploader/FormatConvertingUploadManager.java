/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.uploader;

import com.pinterest.secor.uploader.UploadManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.ReflectionUtil;

import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * Manages uploads using the configured upload manager
 * Converts the local files to the specified format before upload
 *
 * @author Jason Butterfield (jason.butterfield@outreach.io)
 */
public class FormatConvertingUploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(FormatConvertingUploadManager.class);

    private UploadManager mUploadManager;

    public FormatConvertingUploadManager(SecorConfig config) throws Exception {
        super(config);
        mUploadManager = ReflectionUtil.createUploadManager(mConfig.getInnerUploadManagerClass(), mConfig);
    }

    @Override
    public Handle<?> upload(LogFilePath localPath) throws Exception {
        // convert the file from the internal format to the external format
        LogFilePath convertedFilePath = localPath.withPrefix("convertedForUpload");

        FileReader reader = null;
        FileWriter writer = null;
        int copiedMessages = 0;

        try {
            CompressionCodec codec = null;
            String extension = "";
            if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
                codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
                extension = codec.getDefaultExtension();
            }

            reader = createReader(localPath, codec);
            writer = createWriter(convertedFilePath, codec);

            KeyValue keyVal;
            while ((keyVal = reader.next()) != null) {
              writer.write(keyVal);
              copiedMessages++;
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        }

        LOG.info("converted {} messages from {} to {}",
            copiedMessages,
            localPath.getLogFilePath(),
            convertedFilePath.getLogFilePath()
        );

        return new TempFileUploadHandle(mUploadManager.upload(convertedFilePath), convertedFilePath);
    }

    /**
     * This method is intended to be overwritten in tests.
     * @param srcPath source Path
     * @param codec compression codec
     * @return FileReader created file reader
     * @throws Exception on error
     */
    protected FileReader createReader(LogFilePath srcPath, CompressionCodec codec) throws Exception {
        return ReflectionUtil.createFileReader(
          mConfig.getFileReaderWriterFactory(),
          srcPath,
          codec,
          mConfig
        );
    }

    protected FileWriter createWriter(LogFilePath dstPath, CompressionCodec codec) throws Exception {
        FileWriter writer = ReflectionUtil.createFileWriter(
          mConfig.getDestinationFileReaderWriterFactory(),
          dstPath,
          codec,
          mConfig
        );
        FileUtil.deleteOnExit(dstPath.getLogFilePath());
        FileUtil.deleteOnExit(dstPath.getLogFileCrcPath());
        return writer;
    }
}
