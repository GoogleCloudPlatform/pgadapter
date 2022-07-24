// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Entity, PrimaryColumn, Column } from "typeorm"

@Entity("all_types")
export class AllTypes {

  @PrimaryColumn({primaryKeyConstraintName: 'PK_all_types', type: 'bigint'})
  col_bigint: number

  @Column({type: 'bool'})
  col_bool: boolean

  @Column({type: 'bytea'})
  col_bytea: Buffer

  @Column({type: 'float8'})
  col_float8: number

  @Column({type: 'int'})
  col_int: number

  @Column({type: 'numeric'})
  col_numeric: number

  @Column({type: 'timestamptz'})
  col_timestamptz: Date

  @Column({type: 'date'})
  col_date: string

  @Column({type: 'varchar', length: 100})
  col_varchar: string
}
