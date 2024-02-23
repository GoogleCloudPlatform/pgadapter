"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.initModels = exports.TicketSale = exports.Concert = exports.Venue = exports.Track = exports.Album = exports.Singer = void 0;
const sequelize_1 = require("sequelize");
class Singer extends sequelize_1.Model {
}
exports.Singer = Singer;
class Album extends sequelize_1.Model {
}
exports.Album = Album;
class Track extends sequelize_1.Model {
}
exports.Track = Track;
class Venue extends sequelize_1.Model {
}
exports.Venue = Venue;
class Concert extends sequelize_1.Model {
}
exports.Concert = Concert;
class TicketSale extends sequelize_1.Model {
}
exports.TicketSale = TicketSale;
function initModels(sequelize) {
    Singer.init({
        id: {
            type: sequelize_1.DataTypes.BIGINT,
            primaryKey: true,
            autoIncrement: true,
        },
        firstName: {
            type: sequelize_1.DataTypes.STRING,
        },
        lastName: {
            type: sequelize_1.DataTypes.STRING,
        },
        fullName: {
            type: sequelize_1.DataTypes.STRING,
        },
        active: {
            type: sequelize_1.DataTypes.BOOLEAN,
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize });
    Album.init({
        id: {
            type: sequelize_1.DataTypes.BIGINT,
            primaryKey: true,
            autoIncrement: true,
        },
        title: {
            type: sequelize_1.DataTypes.STRING,
        },
        SingerId: {
            type: sequelize_1.DataTypes.INTEGER,
        },
        marketingBudget: {
            type: sequelize_1.DataTypes.DECIMAL,
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize });
    // Note: Track is interleaved in Album.
    Track.init({
        id: {
            type: sequelize_1.DataTypes.BIGINT,
            primaryKey: true,
            // Track.id is not an auto-increment column, because it has to have the same value
            // as the Album that it belongs to.
            autoIncrement: false,
        },
        trackNumber: {
            type: sequelize_1.DataTypes.INTEGER,
            primaryKey: true,
            autoIncrement: false,
        },
        title: {
            type: sequelize_1.DataTypes.STRING,
        },
        sampleRate: {
            type: sequelize_1.DataTypes.DOUBLE,
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize });
    Venue.init({
        id: {
            type: sequelize_1.DataTypes.BIGINT,
            primaryKey: true,
            autoIncrement: true,
        },
        name: {
            type: sequelize_1.DataTypes.STRING,
        },
        description: {
            type: sequelize_1.DataTypes.STRING,
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize });
    Concert.init({
        id: {
            type: sequelize_1.DataTypes.BIGINT,
            primaryKey: true,
            autoIncrement: true,
        },
        VenueId: {
            type: sequelize_1.DataTypes.BIGINT,
        },
        SingerId: {
            type: sequelize_1.DataTypes.BIGINT,
        },
        name: {
            type: sequelize_1.DataTypes.STRING,
        },
        startTime: {
            type: sequelize_1.DataTypes.DATE,
        },
        endTime: {
            type: sequelize_1.DataTypes.DATE,
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize });
    TicketSale.init({
        id: {
            type: sequelize_1.DataTypes.BIGINT,
            primaryKey: true,
            autoIncrement: true,
        },
        ConcertId: {
            type: sequelize_1.DataTypes.BIGINT,
        },
        customerName: {
            type: sequelize_1.DataTypes.STRING,
        },
        price: {
            type: sequelize_1.DataTypes.DECIMAL,
        },
        seats: {
            type: sequelize_1.DataTypes.ARRAY(sequelize_1.DataTypes.STRING),
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize });
    Singer.hasMany(Album);
    Album.belongsTo(Singer);
    // Tracks is interleaved in Albums.
    // This means that they both share the first primary key column `id`.
    // In addition, Tracks has a `trackNumber` primary key column.
    // The reference ('foreign key') column from Track to Album is therefore the `id` column.
    Album.hasMany(Track, { foreignKey: "id" });
    Track.belongsTo(Album, { foreignKey: "id" });
    Venue.hasMany(Concert);
    Concert.belongsTo(Venue);
    Concert.hasMany(TicketSale);
    TicketSale.belongsTo(Concert);
}
exports.initModels = initModels;
