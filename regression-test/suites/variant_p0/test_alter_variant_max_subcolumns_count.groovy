// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Changing variant_max_subcolumns_count in place keeps every row readable through loads and
// compactions on both sides of the change, with and without an index on the column.
suite("test_alter_variant_max_subcolumns_count") {
    def waitAlter = { tableName ->
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
    }
    def variantType = { int maxSubcolumns ->
        return """variant<'event.code' : bigint, 'src.*' : string,
                properties("variant_max_subcolumns_count" = "${maxSubcolumns}")>"""
    }
    // Rows with 12 distinct leaf paths besides the typed ones, so a cap of 3 sends most of them to
    // the sparse column and a cap of 8 materializes some of those.
    def load = { tableName, refName, int from, int to ->
        def values = []
        def refs = []
        for (int k = from; k < to; k++) {
            def json = """{"event":{"code":${k}},"src":{"ip":"10.0.0.${k % 250}"},"a${k % 12}":${k},"b${(k + 5) % 12}":"s${k}"}"""
            values << "(${k}, '${json}')"
            refs << "(${k}, '${json}')"
        }
        sql "INSERT INTO ${tableName} VALUES ${values.join(',')}"
        sql "INSERT INTO ${refName} VALUES ${refs.join(',')}"
    }
    def check = { tableName, refName, String stage ->
        def got = sql """SELECT k, cast(v['event']['code'] as bigint), cast(v['src']['ip'] as string),
                cast(v['a3'] as bigint), cast(v['b7'] as string), cast(v['a11'] as bigint)
                FROM ${tableName} ORDER BY k"""
        def want = sql """SELECT k, get_json_bigint(s, '\$.event.code'), get_json_string(s, '\$.src.ip'),
                get_json_bigint(s, '\$.a3'), get_json_string(s, '\$.b7'), get_json_bigint(s, '\$.a11')
                FROM ${refName} ORDER BY k"""
        assertEquals(want, got, "${tableName} ${stage}")
        def gotHits = sql """SELECT count() FROM ${tableName} WHERE cast(v['b7'] as string) = 's2'"""
        def wantHits = sql """SELECT count() FROM ${refName} WHERE get_json_string(s, '\$.b7') = 's2'"""
        assertEquals(wantHits, gotHits, "${tableName} ${stage} filter")
    }

    for (boolean withIndex : [false, true]) {
        def tableName = "test_alter_variant_max_subcolumns_count_${withIndex ? 'idx' : 'noidx'}"
        def refName = "${tableName}_ref"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql "DROP TABLE IF EXISTS ${refName}"
        def index = withIndex ? """, INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "unicode")""" : ""
        sql """
            CREATE TABLE ${tableName} (
                k bigint,
                v ${variantType(3)}
                ${index}
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        sql """
            CREATE TABLE ${refName} (k bigint, s string)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        load(tableName, refName, 0, 40)
        load(tableName, refName, 40, 80)
        check(tableName, refName, "before alter")

        // Raising the cap restates the same typed paths.
        sql "ALTER TABLE ${tableName} MODIFY COLUMN v ${variantType(8)} NULL"
        waitAlter(tableName)
        def createSql = sql "SHOW CREATE TABLE ${tableName}"
        assertTrue(createSql[0][1].contains('"variant_max_subcolumns_count" = "8"'), createSql[0][1])
        check(tableName, refName, "after raise")

        load(tableName, refName, 80, 120)
        load(tableName, refName, 120, 160)
        check(tableName, refName, "after raise and load")
        trigger_and_wait_compaction(tableName, "cumulative")
        check(tableName, refName, "after raise and compaction")

        // Lowering it again.
        sql "ALTER TABLE ${tableName} MODIFY COLUMN v ${variantType(2)} NULL"
        waitAlter(tableName)
        load(tableName, refName, 160, 200)
        load(tableName, refName, 200, 240)
        trigger_and_wait_compaction(tableName, "cumulative")
        check(tableName, refName, "after lower and compaction")
        trigger_and_wait_compaction(tableName, "full")
        check(tableName, refName, "after full compaction")

        // 0 means no sparse column; the change to or from it is refused, as is a changed typed path.
        test {
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v ${variantType(0)} NULL"
            exception "between 0 and a positive value"
        }
        test {
            sql """ALTER TABLE ${tableName} MODIFY COLUMN v variant<'event.code' : int, 'src.*' : string,
                    properties("variant_max_subcolumns_count" = "8")> NULL"""
            exception "variant schema templates"
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql "DROP TABLE IF EXISTS ${refName}"
    }
}
