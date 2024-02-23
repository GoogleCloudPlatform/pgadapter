// Copyright 2024 Google LLC
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

import {
  CreationOptional,
  DataTypes,
  HasManyAddAssociationMixin,
  HasManyGetAssociationsMixin,
  HasOneGetAssociationMixin,
  HasOneSetAssociationMixin,
  InferAttributes,
  InferCreationAttributes,
  Model,
  Sequelize
} from 'sequelize';

export class Singer extends Model<InferAttributes<Singer>, InferCreationAttributes<Singer>> {
  declare id: number;
  declare firstName: string;
  declare lastName: string;
  declare fullName: string;
  declare active: boolean;
  declare createdAt: CreationOptional<Date>;
  declare updatedAt: CreationOptional<Date>;

  declare getAlbums: HasManyGetAssociationsMixin<Album>;
}

export class Album extends Model<InferAttributes<Album>, InferCreationAttributes<Album>> {
  declare id: number;
  declare title: string;
  declare SingerId: number;
  declare marketingBudget: number;
  declare createdAt: CreationOptional<Date>;
  declare updatedAt: CreationOptional<Date>;

  declare getSinger: HasOneGetAssociationMixin<Singer>;
  declare setSinger: HasOneSetAssociationMixin<Album, Singer>;
  declare getTracks: HasManyGetAssociationsMixin<Track>;
  declare addTrack: HasManyAddAssociationMixin<Track, Album>;
}

export class Track extends Model<InferAttributes<Track>, InferCreationAttributes<Track>> {
  declare id: number;
  declare trackNumber: number;
  declare title: string;
  declare sampleRate: number;
  declare createdAt: CreationOptional<Date>;
  declare updatedAt: CreationOptional<Date>;

  declare getAlbum: HasOneGetAssociationMixin<Album>;
}

export class Venue extends Model<InferAttributes<Venue>, InferCreationAttributes<Venue>> {
  declare id: number;
  declare name: string;
  // description is mapped to a JSONB column and can be used as an object.
  declare description: string;
  declare createdAt: CreationOptional<Date>;
  declare updatedAt: CreationOptional<Date>;
}

export class Concert extends Model<InferAttributes<Concert>, InferCreationAttributes<Concert>> {
  declare id: number;
  declare VenueId: number;
  declare SingerId: number;
  declare name: string;
  declare startTime: Date;
  declare endTime: Date;
  declare createdAt: CreationOptional<Date>;
  declare updatedAt: CreationOptional<Date>;
}

export class TicketSale extends Model<InferAttributes<TicketSale>, InferCreationAttributes<TicketSale>> {
  declare id: number;
  declare ConcertId: number;
  declare customerName: string;
  declare price: number;
  declare seats: string[];
  declare createdAt: CreationOptional<Date>;
  declare updatedAt: CreationOptional<Date>;
}

export function initModels(sequelize: Sequelize) {
  Singer.init({
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true,
    },
    firstName: {
      type: DataTypes.STRING,
    },
    lastName: {
      type: DataTypes.STRING,
    },
    fullName: {
      type: DataTypes.STRING,
    },
    active: {
      type: DataTypes.BOOLEAN,
    },
    createdAt: {
      type: DataTypes.DATE,
    },
    updatedAt: {
      type: DataTypes.DATE,
    }
  }, {sequelize});

  Album.init({
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true,
    },
    title: {
      type: DataTypes.STRING,
    },
    SingerId: {
      type: DataTypes.INTEGER,
    },
    marketingBudget: {
      type: DataTypes.DECIMAL,
    },
    createdAt: {
      type: DataTypes.DATE,
    },
    updatedAt: {
      type: DataTypes.DATE,
    }
  }, {sequelize});

  // Note: Track is interleaved in Album.
  Track.init({
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      // Track.id is not an auto-increment column, because it has to have the same value
      // as the Album that it belongs to.
      autoIncrement: false,
    },
    trackNumber: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: false,
    },
    title: {
      type: DataTypes.STRING,
    },
    sampleRate: {
      type: DataTypes.DOUBLE,
    },
    createdAt: {
      type: DataTypes.DATE,
    },
    updatedAt: {
      type: DataTypes.DATE,
    }
  }, {sequelize});

  Venue.init({
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true,
    },
    name: {
      type: DataTypes.STRING,
    },
    description: {
      type: DataTypes.STRING,
    },
    createdAt: {
      type: DataTypes.DATE,
    },
    updatedAt: {
      type: DataTypes.DATE,
    }
  }, {sequelize})

  Concert.init({
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true,
    },
    VenueId: {
      type: DataTypes.BIGINT,
    },
    SingerId: {
      type: DataTypes.BIGINT,
    },
    name: {
      type: DataTypes.STRING,
    },
    startTime: {
      type: DataTypes.DATE,
    },
    endTime: {
      type: DataTypes.DATE,
    },
    createdAt: {
      type: DataTypes.DATE,
    },
    updatedAt: {
      type: DataTypes.DATE,
    }
  }, {sequelize})

  TicketSale.init({
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true,
    },
    ConcertId: {
      type: DataTypes.BIGINT,
    },
    customerName: {
      type: DataTypes.STRING,
    },
    price: {
      type: DataTypes.DECIMAL,
    },
    seats: {
      type: DataTypes.ARRAY(DataTypes.STRING),
    },
    createdAt: {
      type: DataTypes.DATE,
    },
    updatedAt: {
      type: DataTypes.DATE,
    }
  }, {sequelize})

  Singer.hasMany(Album);
  Album.belongsTo(Singer);

  // Tracks is interleaved in Albums.
  // This means that they both share the first primary key column `id`.
  // In addition, Tracks has a `trackNumber` primary key column.
  // The reference ('foreign key') column from Track to Album is therefore the `id` column.
  Album.hasMany(Track, {foreignKey: "id"});
  Track.belongsTo(Album, {foreignKey: "id"});

  Venue.hasMany(Concert);
  Concert.belongsTo(Venue);

  Concert.hasMany(TicketSale);
  TicketSale.belongsTo(Concert);
}
