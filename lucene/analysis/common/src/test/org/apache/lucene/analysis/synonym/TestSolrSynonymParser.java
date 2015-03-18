package org.apache.lucene.analysis.synonym;

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

import java.io.StringReader;
import java.text.ParseException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

/**
 * Tests parser for the Solr synonyms format
 * @lucene.experimental
 */
public class TestSolrSynonymParser extends BaseTokenStreamTestCase {
  
  /** Tests some simple examples from the solr wiki */
  public void testSimple() throws Exception {
    String testFile = 
    "i-pod, ipod, ipoooood\n" + 
    "foo => foo bar\n" +
    "foo => baz\n" +
    "this test, that testing";
    
    Analyzer analyzer = new MockAnalyzer(random());
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    parser.parse(new StringReader(testFile));
    final SynonymMap map = parser.build();
    analyzer.close();
    
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
        return new TokenStreamComponents(tokenizer, new SynonymFilter(tokenizer, map, true));
      }
    };
    
    assertAnalyzesTo(analyzer, "ball", 
        new String[] { "ball" },
        new int[] { 1 });
    
    assertAnalyzesTo(analyzer, "i-pod",
        new String[] { "i-pod", "ipod", "ipoooood" },
        new int[] { 1, 0, 0 });
    
    assertAnalyzesTo(analyzer, "foo",
        new String[] { "foo", "baz", "bar" },
        new int[] { 1, 0, 1 });
    
    assertAnalyzesTo(analyzer, "this test",
        new String[] { "this", "that", "test", "testing" },
        new int[] { 1, 0, 1, 0 });
    analyzer.close();
  }
  
  /** parse a syn file with bad syntax */
  public void testInvalidDoubleMap() throws Exception {
    String testFile = "a => b => c";
    Analyzer analyzer = new MockAnalyzer(random());
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      // expected exc
    }
    analyzer.close();
  }
  
  /** parse a syn file with bad syntax */
  public void testInvalidAnalyzesToNothingOutput() throws Exception {
    String testFile = "a => 1"; 
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, false);
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      // expected exc
    }
    analyzer.close();
  }
  
  /** parse a syn file with bad syntax */
  public void testInvalidAnalyzesToNothingInput() throws Exception {
    String testFile = "1 => a";
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, false);
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      // expected exc
    }
    analyzer.close();
  }
  
  /** parse a syn file with bad syntax */
  public void testInvalidPositionsInput() throws Exception {
    String testFile = "testola => the test";
    Analyzer analyzer = new EnglishAnalyzer();
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      // expected exc
    }
    analyzer.close();
  }
  
  /** parse a syn file with bad syntax */
  public void testInvalidPositionsOutput() throws Exception {
    String testFile = "the test => testola";
    Analyzer analyzer = new EnglishAnalyzer();
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      // expected exc
    }
    analyzer.close();
  }
  
  /** parse a syn file with some escaped syntax chars */
  public void testEscapedStuff() throws Exception {
    String testFile = 
      "a\\=>a => b\\=>b\n" +
      "a\\,a => b\\,b";
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    parser.parse(new StringReader(testFile));
    final SynonymMap map = parser.build();
    analyzer.close();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
        return new TokenStreamComponents(tokenizer, new SynonymFilter(tokenizer, map, false));
      }
    };
    
    assertAnalyzesTo(analyzer, "ball", 
        new String[] { "ball" },
        new int[] { 1 });
    
    assertAnalyzesTo(analyzer, "a=>a",
        new String[] { "b=>b" },
        new int[] { 1 });
    
    assertAnalyzesTo(analyzer, "a,a",
        new String[] { "b,b" },
        new int[] { 1 });
    analyzer.close();
  }
}
