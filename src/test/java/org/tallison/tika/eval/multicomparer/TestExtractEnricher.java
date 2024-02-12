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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

public class TestExtractEnricher {

    @Test
    public void testOne() throws Exception {
        Path path = Paths.get("/home/tallison/data/cc-html/cc-html-html/00" +
                "/00BDDF2561A802306B9BC56C063047A0761226E6CE8F85F377D4E91C2224761B.json");


        List<Metadata> input;
        try (Reader reader =
                     new BufferedReader(new InputStreamReader(
                             TestExtractEnricher.class.getResourceAsStream("/test-documents/test" +
                                     "-one.json"), StandardCharsets.UTF_8))) {
            input = JsonMetadataList.fromJson(reader);

        }
        List<Metadata> metadataList = new ExtractEnricher().enrich(input);
        Metadata m = metadataList.get(0);
        assertEquals("jpn", m.get("detected_lang"));
        assertEquals("ja", m.get("header_lang"));
        assertEquals("text/html", m.get("mime"));
        assertEquals("Shift_JIS", m.get("charset"));
        assertEquals("76", m.get("parse_time_ms"));
        assertEquals("4982", m.get("num_alpha_tokens"));
        assertEquals("5018", m.get("num_tokens"));
        assertEquals("3059", m.get("num_common_tokens"));
        assertEquals(0.3859, Float.parseFloat(m.get("oov")), 0.01f);
    }

}
