/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.time.OffsetDateTime;

@Internal
public final class OffsetDateTimeComparator extends BasicTypeComparator<OffsetDateTime> {

    private static final long serialVersionUID = 1L;

    public OffsetDateTimeComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        return compareSerializedDate(firstSource, secondSource, ascendingComparison);
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public int getNormalizeKeyLen() {
        return 12;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < 8;
    }

    @Override
    public void putNormalizedKey(OffsetDateTime record, MemorySegment target, int offset, int numBytes) {
        putNormalizedKeyDate(record, target, offset, numBytes);
    }

    @Override
    public OffsetDateTimeComparator duplicate() {
        return new OffsetDateTimeComparator(ascendingComparison);
    }

    // --------------------------------------------------------------------------------------------
    //                           Static Helpers for OffsetDateTime Comparison
    // --------------------------------------------------------------------------------------------

    public static int compareSerializedDate(
            DataInputView firstSource, DataInputView secondSource, boolean ascendingComparison)
            throws IOException {
        final long l1 = firstSource.readLong();
        final long l2 = secondSource.readLong();
        int comp = (l1 < l2 ? -1 : (l1 == l2 ? 0 : 1));

        if (comp == 0) {
            final int i1 = firstSource.readInt();
            final int i2 = secondSource.readInt();
            comp = (i1 < i2 ? -1 : (i1 == i2 ? 0 : 1));
        }
        return ascendingComparison ? comp : -comp;
    }

    public static void putNormalizedKeyDate(
            OffsetDateTime record, MemorySegment target, int offset, int numBytes) {
        final long value = record.toEpochSecond() - Long.MIN_VALUE;
        final int offsetValue = record.getOffset().getTotalSeconds() - Integer.MIN_VALUE;


        // see IntValue for an explanation of the logic
        if (numBytes == 12) {
            // default case, full normalized key
            target.putLongBigEndian(offset, value);
            target.putIntBigEndian(offset + 8, offsetValue);
        } else if (numBytes < 12) {
            for (int i = 0; numBytes > 0; numBytes--, i++) {
                target.put(offset + i, (byte) (value >>> ((12 - i) << 3)));
            }
        } else {
            target.putLongBigEndian(offset, value);
            for (int i = 8; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }
}
