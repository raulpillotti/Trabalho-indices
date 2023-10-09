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
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
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
var fs = require("fs");
var csvParser = require("csv-parser");
//para estrutura em árvore
var ContentRating;
(function (ContentRating) {
    ContentRating[ContentRating["Everyone"] = 0] = "Everyone";
    ContentRating[ContentRating["Everyone 10+"] = 1] = "Everyone 10+";
    ContentRating[ContentRating["Teen"] = 2] = "Teen";
    ContentRating[ContentRating["Mature 17+"] = 3] = "Mature 17+";
})(ContentRating || (ContentRating = {}));
var CSV_FILE_PATH = 'Google-Playstore.csv';
var BIN_FILE_PATH = 'Google-Playstore.bin';
var INDEXES_FILE_PATH = 'Indexes.json';
var SECUNDARY_INDEXES_FILE_PATH = 'Indexes2.json';
var csvFile = fs.createReadStream(CSV_FILE_PATH);
var wBinFile = fs.createWriteStream(BIN_FILE_PATH, { flags: 'w' });
fs.writeFile(INDEXES_FILE_PATH, '[', 'utf8', function () { return console.log('Limpando arquivo de índices'); });
fs.writeFile(SECUNDARY_INDEXES_FILE_PATH, '[', 'utf8', function () { return console.log('Limpando arquivo de índices secundario'); });
var LINE_SIZE = 1000;
var curRowIdx = 2;
var curBytes = 0;
var inMemoryIndex = [];
var inMemoryIndex2 = [];
csvFile
    //retorna um objeto com {key: nome da coluna, value: linha correspondente}
    //iterando todas as linhas
    .pipe(csvParser())
    .on('data', function (row) {
    //escreve no arquivo de registros binário
    //atribuie uma chave para o registro, a chave é o número da linha no csv
    row.key = curRowIdx++;
    var str = JSON.stringify(row);
    str = str.padEnd(LINE_SIZE - 1, ' ');
    str += '\n';
    var bytes = Buffer.from(str, 'utf-8');
    wBinFile.write(bytes);
    //escreve no arquivo de índices
    var index = { key: row.key, pos: curBytes };
    var indexAsStr = JSON.stringify(index);
    indexAsStr += ',';
    fs.appendFileSync(INDEXES_FILE_PATH, indexAsStr, 'utf8');
    //escreve no arquivo de índices secundario, a chave é o nome do app e aponta para a chave do índice principal
    var secundaryIndex = { key: row['App Name'], pos: row.key };
    var secundaryIndexAsStr = JSON.stringify(secundaryIndex);
    secundaryIndexAsStr += ',';
    fs.appendFileSync(SECUNDARY_INDEXES_FILE_PATH, secundaryIndexAsStr, 'utf8');
    //adiciona nos arquivos de índices em memória com a chave data de lançamento
    inMemoryIndex.push({ key: row.Released, pos: curBytes });
    //adiciona nos arquivos de índices em memória com a chave classificação etária
    var obj = { key: row['Content Rating'], pos: curBytes };
    insertInMemoryIndex2(obj);
    curBytes += LINE_SIZE;
})
    .on('end', function () { return __awaiter(void 0, void 0, void 0, function () {
    var indexFileData, indexFileNewData, secundaryIndexFileData, secundaryIndexFileNewData, ret1, ret2, ret3, ret4, appName, answer;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                console.log(inMemoryIndex2);
                wBinFile.end();
                indexFileData = fs.readFileSync(INDEXES_FILE_PATH, 'utf-8');
                indexFileNewData = indexFileData.slice(0, -1);
                fs.writeFileSync(INDEXES_FILE_PATH, indexFileNewData);
                fs.appendFileSync(INDEXES_FILE_PATH, ']', 'utf-8');
                secundaryIndexFileData = fs.readFileSync(SECUNDARY_INDEXES_FILE_PATH, 'utf-8');
                secundaryIndexFileNewData = secundaryIndexFileData.slice(0, -1);
                fs.writeFileSync(SECUNDARY_INDEXES_FILE_PATH, secundaryIndexFileNewData);
                fs.appendFileSync(SECUNDARY_INDEXES_FILE_PATH, ']', 'utf-8');
                return [4 /*yield*/, findInMainIndexFile(15)];
            case 1:
                ret1 = _a.sent();
                return [4 /*yield*/, findInSecundaryFile('Vibook')];
            case 2:
                ret2 = _a.sent();
                console.log('Pesquisa arquivo de índices principal: ', ret1);
                console.log('Pesquisa arquivo de índices secundario: ', ret2);
                return [4 /*yield*/, findInMemoryIndex('12/24/18')];
            case 3:
                ret3 = _a.sent();
                return [4 /*yield*/, findInMemoryIndex2(ContentRating['Teen'])];
            case 4:
                ret4 = _a.sent();
                console.log('Pesquisa arquivo de índices em memória: ', ret3);
                console.log('Pesquisa arquivo de índices em memória 2: ', ret4);
                appName = 'PowerSwitch';
                console.log("PERGUNTA: Qual o contato de quem criou o app chamado ".concat(appName, "?"));
                return [4 /*yield*/, findInSecundaryFile(appName)];
            case 5:
                answer = _a.sent();
                console.log("RESPOSTA: ", answer['Developer Email']);
                return [2 /*return*/];
        }
    });
}); });
function binarySearch(arr, target) {
    var left = 0;
    var right = arr.length - 1;
    while (left <= right) {
        var mid = Math.floor((left + right) / 2);
        if (arr[mid].key === target) {
            return arr[mid];
        }
        else if (Number(arr[mid].key) < target) {
            left = mid + 1;
        }
        else {
            right = mid - 1;
        }
    }
    return null;
}
function findInMainIndexFile(targetRow) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    //lê arquivo de índices e cria objetos  
                    var dataFromIndex = fs.readFileSync(INDEXES_FILE_PATH, 'utf-8');
                    var indexObjs = JSON.parse(dataFromIndex);
                    //procura no arquivo de índices usando a pesquisa binária
                    var indexTarget = binarySearch(indexObjs, targetRow);
                    if (!indexTarget) {
                        reject('Índice inválido.');
                        return;
                    }
                    //pesquisa no arquivo binário do byte inicial do target até + 1000 e cria um objeto 
                    var rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: indexTarget.pos, end: indexTarget.pos + LINE_SIZE });
                    var ret;
                    rBinFile
                        .on('data', function (chunk) {
                        ret = JSON.parse(chunk.toString());
                    })
                        .on('end', function () {
                        rBinFile.close();
                        resolve(ret);
                    });
                })];
        });
    });
}
function findInSecundaryFile(targetAppName) {
    return new Promise(function (resolve, reject) {
        //lê arquivo de índices secundario e cria objetos  
        var dataFromSecundaryIndex = fs.readFileSync(SECUNDARY_INDEXES_FILE_PATH, 'utf-8');
        var secundaryIndexObjs = JSON.parse(dataFromSecundaryIndex);
        //lê arquivo de índices principal e cria objetos  
        var dataFromIndex = fs.readFileSync(INDEXES_FILE_PATH, 'utf-8');
        var indexObjs = JSON.parse(dataFromIndex);
        //procura objeto no arquivo de índices secundário pelo nome
        var target = secundaryIndexObjs.find(function (index) { return index.key == targetAppName; });
        if (!target) {
            reject('Índice inválido.');
            return;
        }
        //procura objeto no arquivo de índices principal pelo "pos" usando a pesquisa binária 
        var secundaryIndexTarget = binarySearch(indexObjs, target.pos);
        if (!secundaryIndexTarget) {
            reject('Índice inválido.');
            return;
        }
        //pesquisa no arquivo binário do byte inicial do target até + 1000 e cria um objeto 
        var rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: secundaryIndexTarget.pos, end: secundaryIndexTarget.pos + LINE_SIZE });
        var ret;
        rBinFile
            .on('data', function (chunk) {
            ret = JSON.parse(chunk.toString());
        })
            .on('end', function () {
            rBinFile.close();
            resolve(ret);
        });
    });
}
function findInMemoryIndex(target) {
    return __awaiter(this, void 0, void 0, function () {
        var targets, promises;
        return __generator(this, function (_a) {
            targets = inMemoryIndex.filter(function (index) { return index.key == target; });
            promises = targets.map(function (target) {
                return new Promise(function (resolve, reject) {
                    var rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: target.pos, end: target.pos + LINE_SIZE });
                    var ret;
                    rBinFile
                        .on('data', function (chunk) {
                        ret = JSON.parse(chunk.toString());
                    })
                        .on('end', function () {
                        rBinFile.close();
                        resolve(ret);
                    });
                });
            });
            //concorrentemente resolve todas promises e retorna os objetos
            return [2 /*return*/, Promise.all(promises)];
        });
    });
}
function insertInMemoryIndex2(value) {
    var obj = { key: value.key, pos: value.pos, parents: [], children: [] };
    inMemoryIndex2.push(obj);
    //adiciona como filho/pai de acordo com o valor da chave para cada objeto
    inMemoryIndex2.forEach(function (reg) {
        var regKeyVal = ContentRating[reg.key];
        var valueKeyVal = ContentRating[obj.key];
        if (regKeyVal > valueKeyVal) {
            reg.children.push(obj);
            obj.parents.push(reg);
        }
        else if (regKeyVal < valueKeyVal) {
            reg.parents.push(obj);
            obj.children.push(reg);
        }
    });
}
function findInMemoryIndex2(target) {
    var found = null;
    var find = function (targets) {
        //se o ContentRating do target for igual ao ContentRating da iteração atual retorna
        //se for maior chama a função recursivamente para os pais e se for menor para os filhos
        for (var i = 0; i < targets.length; i++) {
            var keyVal = ContentRating[targets[i].key];
            if (target == keyVal) {
                found = targets[i];
                break;
            }
            else if (target > keyVal) {
                find(targets[i].parents);
            }
            else if (target < keyVal) {
                find(targets[i].children);
            }
        }
    };
    find(inMemoryIndex2);
    //pesquisa no arquivo binário do byte inicial do target até + 1000 e retorna uma Promise com o objeto
    return new Promise(function (resolve, reject) {
        if (!found)
            reject('Índices em memoria 2: índice não encontrado');
        var idx = found;
        var rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: idx.pos, end: idx.pos + LINE_SIZE });
        var ret;
        rBinFile
            .on('data', function (chunk) {
            ret = JSON.parse(chunk.toString());
        })
            .on('end', function () {
            rBinFile.close();
            resolve(ret);
        });
    });
}
