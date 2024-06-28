"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a, _b, _c, _d;
Object.defineProperty(exports, "__esModule", { value: true });
exports.saveToS3 = exports.copyS3Folder = exports.fetchS3Folder = void 0;
const client_s3_1 = require("@aws-sdk/client-s3");
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const aws_sdk_1 = require("aws-sdk");
const s3Client = new client_s3_1.S3Client({
    region: "ap-south-1",
    credentials: {
        accessKeyId: (_a = process.env.AWS_ACCESS_KEY_ID) !== null && _a !== void 0 ? _a : "",
        secretAccessKey: (_b = process.env.AWS_SECRET_ACCESS_KEY) !== null && _b !== void 0 ? _b : ""
    }
});
const s3 = new aws_sdk_1.S3({
    accessKeyId: (_c = process.env.AWS_ACCESS_KEY_ID) !== null && _c !== void 0 ? _c : "",
    secretAccessKey: (_d = process.env.AWS_SECRET_ACCESS_KEY) !== null && _d !== void 0 ? _d : "",
});
const streamToBuffer = (stream) => __awaiter(void 0, void 0, void 0, function* () {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
});
const fetchS3Folder = (key, localPath) => __awaiter(void 0, void 0, void 0, function* () {
    var _e;
    try {
        const params = {
            Bucket: (_e = process.env.S3_BUCKET) !== null && _e !== void 0 ? _e : "ideasy",
            Prefix: key
        };
        const response = yield s3Client.send(new client_s3_1.ListObjectsV2Command(params));
        if (response.Contents) {
            yield Promise.all(response.Contents.map((file) => __awaiter(void 0, void 0, void 0, function* () {
                var _f;
                const fileKey = file.Key;
                if (fileKey) {
                    const getObjectParams = {
                        Bucket: (_f = process.env.S3_BUCKET) !== null && _f !== void 0 ? _f : "ideasy",
                        Key: fileKey
                    };
                    const data = yield s3Client.send(new client_s3_1.GetObjectCommand(getObjectParams));
                    if (data.Body) {
                        const fileData = data.Body;
                        const filePath = `${localPath}/${fileKey.replace(key, "")}`;
                        const fileBuffer = yield streamToBuffer(data.Body);
                        yield writeFile(filePath, fileBuffer);
                        console.log(`Downloaded ${fileKey} to ${filePath}`);
                    }
                }
            })));
        }
    }
    catch (error) {
        console.error('Error fetching folder:', error);
    }
});
exports.fetchS3Folder = fetchS3Folder;
function copyS3Folder(sourcePrefix, destinationPrefix, continuationToken) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const listParams = {
                Bucket: "ideasy",
                Prefix: sourcePrefix,
                ContinuationToken: continuationToken
            };
            const listedObjects = yield s3Client.send(new client_s3_1.ListObjectsV2Command(listParams));
            if (!listedObjects.Contents || listedObjects.Contents.length === 0)
                return;
            yield Promise.all(listedObjects.Contents.map((object) => __awaiter(this, void 0, void 0, function* () {
                if (!object.Key)
                    return;
                const destinationKey = object.Key.replace(sourcePrefix, destinationPrefix);
                const copyParams = {
                    Bucket: "ideasy",
                    CopySource: `ideasy/${object.Key}`,
                    Key: destinationKey
                };
                yield s3Client.send(new client_s3_1.CopyObjectCommand(copyParams));
                console.log(`Copied ${object.Key} to ${destinationKey}`);
            })));
            if (listedObjects.IsTruncated) {
                yield copyS3Folder(sourcePrefix, destinationPrefix, listedObjects.NextContinuationToken);
            }
        }
        catch (error) {
            console.error('Error copying folder:', error);
        }
    });
}
exports.copyS3Folder = copyS3Folder;
function writeFile(filePath, fileData) {
    return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
        yield createFolder(path_1.default.dirname(filePath));
        fs_1.default.writeFile(filePath, fileData, (err) => {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    }));
}
function createFolder(dirName) {
    return new Promise((resolve, reject) => {
        fs_1.default.mkdir(dirName, { recursive: true }, (err) => {
            if (err) {
                return reject(err);
            }
            resolve();
        });
    });
}
const saveToS3 = (key, filePath, content) => __awaiter(void 0, void 0, void 0, function* () {
    var _g;
    console.log(`${key}${filePath}`);
    const params = {
        Bucket: (_g = process.env.S3_BUCKET) !== null && _g !== void 0 ? _g : "ideasy",
        Key: `${key}${filePath}`,
        Body: content
    };
    yield s3.putObject(params).promise();
});
exports.saveToS3 = saveToS3;
