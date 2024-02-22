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
exports.initModels = exports.Album = exports.Singer = void 0;
const sequelize_1 = require("sequelize");
const init_1 = require("../src/init");
class Singer extends sequelize_1.Model {
}
exports.Singer = Singer;
class Album extends sequelize_1.Model {
}
exports.Album = Album;
function initModels() {
    Singer.init({
        id: {
            type: sequelize_1.DataTypes.INTEGER,
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
            type: sequelize_1.DataTypes.VIRTUAL,
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
    }, { sequelize: init_1.sequelize });
    Album.init({
        id: {
            type: sequelize_1.DataTypes.INTEGER,
            primaryKey: true,
            autoIncrement: true,
        },
        title: {
            type: sequelize_1.DataTypes.STRING,
        },
        SingerId: {
            type: sequelize_1.DataTypes.INTEGER,
        },
        createdAt: {
            type: sequelize_1.DataTypes.DATE,
        },
        updatedAt: {
            type: sequelize_1.DataTypes.DATE,
        }
    }, { sequelize: init_1.sequelize });
    Singer.hasMany(Album);
    Album.belongsTo(Singer);
}
exports.initModels = initModels;
