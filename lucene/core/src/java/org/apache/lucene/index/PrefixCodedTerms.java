package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Prefix codes term instances (prefixes are shared)
 * @lucene.experimental
 */
class PrefixCodedTerms implements Accountable {
  final RAMFile buffer;
  private long delGen;

  private PrefixCodedTerms(RAMFile buffer) {
    this.buffer = buffer;
  }

  @Override
  public long ramBytesUsed() {
    return buffer.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  /** Records del gen for this packet. */
  public void setDelGen(long delGen) {
    this.delGen = delGen;
  }
  
  /** Builds a PrefixCodedTerms: call add repeatedly, then finish. */
  public static class Builder {
    private RAMFile buffer = new RAMFile();
    private RAMOutputStream output = new RAMOutputStream(buffer, false);
    private Term lastTerm = new Term("");
    private BytesRefBuilder lastTermBytes = new BytesRefBuilder();

    /** add a term */
    public void add(Term term) {
      assert lastTerm.equals(new Term("")) || term.compareTo(lastTerm) > 0;

      try {
        int prefix = sharedPrefix(lastTerm.bytes, term.bytes);
        int suffix = term.bytes.length - prefix;
        if (term.field.equals(lastTerm.field)) {
          output.writeVInt(prefix << 1);
        } else {
          output.writeVInt(prefix << 1 | 1);
          output.writeString(term.field);
        }
        output.writeVInt(suffix);
        output.writeBytes(term.bytes.bytes, term.bytes.offset + prefix, suffix);
        lastTermBytes.copyBytes(term.bytes);
        lastTerm.bytes = lastTermBytes.get();
        lastTerm.field = term.field;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /** return finalized form */
    public PrefixCodedTerms finish() {
      try {
        output.close();
        return new PrefixCodedTerms(buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    private int sharedPrefix(BytesRef term1, BytesRef term2) {
      int pos1 = 0;
      int pos1End = pos1 + Math.min(term1.length, term2.length);
      int pos2 = 0;
      while(pos1 < pos1End) {
        if (term1.bytes[term1.offset + pos1] != term2.bytes[term2.offset + pos2]) {
          return pos1;
        }
        pos1++;
        pos2++;
      }
      return pos1;
    }
  }

  public static class TermIterator extends FieldTermIterator {
    final IndexInput input;
    final BytesRefBuilder builder = new BytesRefBuilder();
    final BytesRef bytes = builder.get();
    final long end;
    final long delGen;
    String field = "";

    public TermIterator(long delGen, RAMFile buffer) {
      try {
        input = new RAMInputStream("MergedPrefixCodedTermsIterator", buffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      end = input.length();
      this.delGen = delGen;
    }

    @Override
    public BytesRef next() {
      if (input.getFilePointer() < end) {
        try {
          int code = input.readVInt();
          boolean newField = (code & 1) != 0;
          if (newField) {
            field = input.readString();
          }
          int prefix = code >>> 1;
          int suffix = input.readVInt();
          readTermBytes(prefix, suffix);
          return bytes;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        field = null;
        return null;
      }
    }

    // TODO: maybe we should freeze to FST or automaton instead?
    private void readTermBytes(int prefix, int suffix) throws IOException {
      builder.grow(prefix + suffix);
      input.readBytes(builder.bytes(), prefix, suffix);
      builder.setLength(prefix + suffix);
    }

    @Override
    public String field() {
      return field;
    }

    @Override
    public long delGen() {
      return delGen;
    }
  }

  public TermIterator iterator() {
    return new TermIterator(delGen, buffer);
  }
}
