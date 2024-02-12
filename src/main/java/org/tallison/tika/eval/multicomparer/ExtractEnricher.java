/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.tika.eval.multicomparer;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.tika.eval.core.metadata.TikaEvalMetadataFilter;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

public class ExtractEnricher {
    TikaEvalMetadataFilter tikaEvalMetadataFilter = new TikaEvalMetadataFilter();

    private final static Map<String, String> MAPPINGS = Map.of(
            "X-TIKA:detectedEncoding", "charset",
            "X-TIKA:parse_time_millis", "parse_time_ms",
            "Content-Language","header_lang",
            "tika-eval:lang", "detected_lang",
            "tika-eval:numTokens", "num_tokens",
            "tika-eval:numAlphaTokens", "num_alpha_tokens",
            "tika-eval:numCommonTokens", "num_common_tokens",
            "tika-eval:oov", "oov"
    );
    public List<Metadata> enrich(List<Metadata> metadataList) throws TikaException {
        Metadata input = metadataList.get(0);
        tikaEvalMetadataFilter.filter(input);
        String contentType = input.get(Metadata.CONTENT_TYPE);
        String mime = "";
        if (! StringUtils.isAllBlank(contentType)) {
            MediaType t = MediaType.parse(contentType);
            if (t != null) {
                mime = t.getType() + "/" + t.getSubtype();
            }
        }
        Metadata ret = new Metadata();
        ret.add("mime", mime);
        for (Map.Entry<String, String> e : MAPPINGS.entrySet()) {
            String val = input.get(e.getKey());
            if (!StringUtils.isAllBlank(val)) {
                ret.set(e.getValue(), val);
            }
        }
        return List.of(ret);
    }
}
