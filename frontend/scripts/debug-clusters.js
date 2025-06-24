"use strict";
var __makeTemplateObject = (this && this.__makeTemplateObject) || function (cooked, raw) {
    if (Object.defineProperty) { Object.defineProperty(cooked, "raw", { value: raw }); } else { cooked.raw = raw; }
    return cooked;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
// scripts/debug-clusters.ts
var dotenv_1 = require("dotenv");
var path_1 = require("path");
var drizzle_orm_1 = require("drizzle-orm");
// Load environment variables
(0, dotenv_1.config)({ path: path_1.default.resolve(process.cwd(), '.env') });
(0, dotenv_1.config)({ path: path_1.default.resolve(process.cwd(), '.env.local') });
// Import after env vars are loaded
var config_1 = require("../src/lib/db/config");
var db_1 = require("../src/lib/db");
var schema_1 = require("../src/lib/db/schema");
function debugClusterData() {
    return __awaiter(this, void 0, void 0, function () {
        var connection, totalComments, withClusterIds, withFullClusterData, sampleClusterData, uniqueClusters, nullPcaInClusters, error_1;
        var _a, _b, _c, _d, _e;
        return __generator(this, function (_f) {
            switch (_f.label) {
                case 0:
                    console.log('🔍 Debugging Cluster Data\n');
                    console.log("Environment: ".concat((_a = config_1.dbConfig.env) === null || _a === void 0 ? void 0 : _a.toUpperCase()));
                    console.log('-------------------\n');
                    _f.label = 1;
                case 1:
                    _f.trys.push([1, 9, , 10]);
                    return [4 /*yield*/, (0, db_1.connectDb)()];
                case 2:
                    connection = _f.sent();
                    if (!connection.success) {
                        console.error('❌ Failed to connect to database');
                        return [2 /*return*/];
                    }
                    return [4 /*yield*/, db_1.db.select({ count: (0, drizzle_orm_1.sql)(templateObject_1 || (templateObject_1 = __makeTemplateObject(["count(*)"], ["count(*)"]))) }).from(schema_1.comments)];
                case 3:
                    totalComments = _f.sent();
                    console.log("Total comments in database: ".concat(((_b = totalComments[0]) === null || _b === void 0 ? void 0 : _b.count) || 0));
                    return [4 /*yield*/, db_1.db
                            .select({ count: (0, drizzle_orm_1.sql)(templateObject_2 || (templateObject_2 = __makeTemplateObject(["count(*)"], ["count(*)"]))) })
                            .from(schema_1.comments)
                            .where((0, drizzle_orm_1.sql)(templateObject_3 || (templateObject_3 = __makeTemplateObject(["", " IS NOT NULL"], ["", " IS NOT NULL"])), schema_1.comments.clusterId))];
                case 4:
                    withClusterIds = _f.sent();
                    console.log("Comments with cluster IDs: ".concat(((_c = withClusterIds[0]) === null || _c === void 0 ? void 0 : _c.count) || 0));
                    return [4 /*yield*/, db_1.db
                            .select({ count: (0, drizzle_orm_1.sql)(templateObject_4 || (templateObject_4 = __makeTemplateObject(["count(*)"], ["count(*)"]))) })
                            .from(schema_1.comments)
                            .where((0, drizzle_orm_1.sql)(templateObject_5 || (templateObject_5 = __makeTemplateObject(["", " IS NOT NULL AND ", " IS NOT NULL AND ", " IS NOT NULL"], ["", " IS NOT NULL AND ", " IS NOT NULL AND ", " IS NOT NULL"])), schema_1.comments.clusterId, schema_1.comments.pcaX, schema_1.comments.pcaY))];
                case 5:
                    withFullClusterData = _f.sent();
                    console.log("Comments with full cluster data (ID + PCA coords): ".concat(((_d = withFullClusterData[0]) === null || _d === void 0 ? void 0 : _d.count) || 0));
                    return [4 /*yield*/, db_1.db
                            .select({
                            id: schema_1.comments.id,
                            clusterId: schema_1.comments.clusterId,
                            pcaX: schema_1.comments.pcaX,
                            pcaY: schema_1.comments.pcaY,
                        })
                            .from(schema_1.comments)
                            .where((0, drizzle_orm_1.sql)(templateObject_6 || (templateObject_6 = __makeTemplateObject(["", " IS NOT NULL"], ["", " IS NOT NULL"])), schema_1.comments.clusterId))
                            .limit(5)];
                case 6:
                    sampleClusterData = _f.sent();
                    console.log('\nSample cluster data:');
                    sampleClusterData.forEach(function (row) {
                        console.log("  ID: ".concat(row.id, ", Cluster: ").concat(row.clusterId, ", PCA: (").concat(row.pcaX, ", ").concat(row.pcaY, ")"));
                    });
                    return [4 /*yield*/, db_1.db
                            .select({
                            clusterId: schema_1.comments.clusterId,
                            count: (0, drizzle_orm_1.sql)(templateObject_7 || (templateObject_7 = __makeTemplateObject(["COUNT(*)"], ["COUNT(*)"]))).mapWith(Number),
                        })
                            .from(schema_1.comments)
                            .where((0, drizzle_orm_1.sql)(templateObject_8 || (templateObject_8 = __makeTemplateObject(["", " IS NOT NULL"], ["", " IS NOT NULL"])), schema_1.comments.clusterId))
                            .groupBy(schema_1.comments.clusterId)];
                case 7:
                    uniqueClusters = _f.sent();
                    console.log("\nUnique clusters: ".concat(uniqueClusters.length));
                    uniqueClusters.slice(0, 5).forEach(function (cluster) {
                        console.log("  Cluster ".concat(cluster.clusterId, ": ").concat(cluster.count, " comments"));
                    });
                    return [4 /*yield*/, db_1.db
                            .select({ count: (0, drizzle_orm_1.sql)(templateObject_9 || (templateObject_9 = __makeTemplateObject(["count(*)"], ["count(*)"]))) })
                            .from(schema_1.comments)
                            .where((0, drizzle_orm_1.sql)(templateObject_10 || (templateObject_10 = __makeTemplateObject(["", " IS NOT NULL AND (", " IS NULL OR ", " IS NULL)"], ["", " IS NOT NULL AND (", " IS NULL OR ", " IS NULL)"])), schema_1.comments.clusterId, schema_1.comments.pcaX, schema_1.comments.pcaY))];
                case 8:
                    nullPcaInClusters = _f.sent();
                    console.log("\nComments with cluster ID but missing PCA coords: ".concat(((_e = nullPcaInClusters[0]) === null || _e === void 0 ? void 0 : _e.count) || 0));
                    return [3 /*break*/, 10];
                case 9:
                    error_1 = _f.sent();
                    console.error('❌ Error during cluster data check:', error_1);
                    return [3 /*break*/, 10];
                case 10:
                    process.exit(0);
                    return [2 /*return*/];
            }
        });
    });
}
// Run the check
debugClusterData();
var templateObject_1, templateObject_2, templateObject_3, templateObject_4, templateObject_5, templateObject_6, templateObject_7, templateObject_8, templateObject_9, templateObject_10;
