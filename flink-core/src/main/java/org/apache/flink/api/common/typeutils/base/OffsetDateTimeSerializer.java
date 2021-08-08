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
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

@Internal
public final class OffsetDateTimeSerializer extends TypeSerializerSingleton<OffsetDateTime> {

    private static final long serialVersionUID = 1L;

    public static final OffsetDateTimeSerializer INSTANCE = new OffsetDateTimeSerializer();

    private static final OffsetDateTime EPOCH = Instant.EPOCH.atOffset(ZoneOffset.UTC);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public OffsetDateTime createInstance() {
        return EPOCH;
    }

    @Override
    public OffsetDateTime copy(OffsetDateTime from) {
        if (from == null) {
            return null;
        }
        return from;
    }

    @Override
    public OffsetDateTime copy(OffsetDateTime from, OffsetDateTime reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 12;
    }

    @Override
    public void serialize(OffsetDateTime record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeLong(Long.MIN_VALUE);
            target.skipBytesToWrite(4);
        } else {
            target.writeLong(record.toEpochSecond());
            target.writeInt(record.getOffset().getTotalSeconds());
        }
    }

    @Override
    public OffsetDateTime deserialize(DataInputView source) throws IOException {
        final long v = source.readLong();
        if (v == Long.MIN_VALUE) {
            return null;
        } else {
            return Instant.ofEpochSecond(v).atOffset(ZoneOffset.ofTotalSeconds(source.readInt()));
        }
    }

    @Override
    public OffsetDateTime deserialize(OffsetDateTime reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        target.writeInt(source.readInt());
    }

    @Override
    public TypeSerializerSnapshot<OffsetDateTime> snapshotConfiguration() {
        return new DateSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class DateSerializerSnapshot extends SimpleTypeSerializerSnapshot<OffsetDateTime> {

        public DateSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
