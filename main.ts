import * as fs from 'fs';
import * as csvParser from 'csv-parser';

//para a busca binária 
interface HasKey {
    key: number | string;
}

//dados da linha no csv
interface RowData extends HasKey {
    'App Name': string;
    'App Id': string;
    Category: string;
    Rating: number;
    'Rating Count': number;
    Installs: string;
    'Minimum Installs': number;
    'Maximum Installs': number;
    Free: boolean;
    Price: number;
    Currency: 'USD' | 'XXX';
    Size: string;
    'Minimum Android': string;
    'Developer Id': string;
    'Developer Website': string;
    'Developer Email': string;
    Released: string;
    'Last Updated': string;
    'Content Rating': ContentRating;
    'Privacy Policy': string;
    'Ad Supported': boolean;
    'In App Purchases': boolean;
    'Editors Choice': boolean;
    'Scrapped Time': string;
}

//para estrutura em árvore
enum ContentRating {
    'Everyone' = 0,
    'Everyone 10+' = 1,
    'Teen' = 2,
    'Mature 17+' = 3,
}

//para arquivo de indices
interface Index extends HasKey {
    pos: number;
}

interface Tree extends Index {
    children: Tree[];
    parents: Tree[];
}

//utilizar o arquivo inteiro está estourando a memória...
const CSV_FILE_PATH = 'teste.csv';

const BIN_FILE_PATH = 'Google-Playstore.bin';
const INDEXES_FILE_PATH = 'Indexes.json';
const SECUNDARY_INDEXES_FILE_PATH = 'Indexes2.json';

const csvFile = fs.createReadStream(CSV_FILE_PATH);
const wBinFile = fs.createWriteStream(BIN_FILE_PATH, { flags: 'w' });

fs.writeFile(INDEXES_FILE_PATH, '[', 'utf8', () => console.log('Limpando arquivo de índices'));
fs.writeFile(SECUNDARY_INDEXES_FILE_PATH, '[', 'utf8', () => console.log('Limpando arquivo de índices secundario'));

const LINE_SIZE = 1000;

let curRowIdx = 2;
let curBytes = 0;

const inMemoryIndex: Index[] = [];
const inMemoryIndex2: Tree[] = [];

csvFile
    //retorna um objeto com {key: nome da coluna, value: linha correspondente}
    //iterando todas as linhas
    .pipe(csvParser())
    .on('data', (row: RowData) => {
        //escreve no arquivo de registros binário
        //atribuie uma chave para o registro, a chave é o número da linha no csv
        row.key = curRowIdx++;
        let str = JSON.stringify(row);
        str = str.padEnd(LINE_SIZE - 1, ' ');
        str += '\n';
        const bytes = Buffer.from(str, 'utf-8');
        wBinFile.write(bytes);

        //escreve no arquivo de índices
        const index: Index = { key: row.key, pos: curBytes };
        let indexAsStr = JSON.stringify(index);
        indexAsStr += ',';
        fs.appendFileSync(INDEXES_FILE_PATH, indexAsStr, 'utf8');

        //escreve no arquivo de índices secundario, a chave é o nome do app e aponta para a chave do índice principal
        const secundaryIndex: Index = { key: row['App Name'], pos: row.key };
        let secundaryIndexAsStr = JSON.stringify(secundaryIndex);
        secundaryIndexAsStr += ',';
        fs.appendFileSync(SECUNDARY_INDEXES_FILE_PATH, secundaryIndexAsStr, 'utf8');

        //adiciona nos arquivos de índices em memória com a chave data de lançamento
        inMemoryIndex.push({ key: row.Released, pos: curBytes });
        //adiciona nos arquivos de índices em memória com a chave classificação etária
        const obj = { key: row['Content Rating'], pos: curBytes };
        insertInMemoryIndex2(obj);

        curBytes += LINE_SIZE;
    })
    .on('end', async () => {
        console.log(inMemoryIndex2);
        wBinFile.end();

        //remover última "," e adicionar prox ]
        const indexFileData = fs.readFileSync(INDEXES_FILE_PATH, 'utf-8');
        const indexFileNewData = indexFileData.slice(0, -1);
        fs.writeFileSync(INDEXES_FILE_PATH, indexFileNewData);
        fs.appendFileSync(INDEXES_FILE_PATH, ']', 'utf-8');

        const secundaryIndexFileData = fs.readFileSync(SECUNDARY_INDEXES_FILE_PATH, 'utf-8');
        const secundaryIndexFileNewData = secundaryIndexFileData.slice(0, -1);
        fs.writeFileSync(SECUNDARY_INDEXES_FILE_PATH, secundaryIndexFileNewData);
        fs.appendFileSync(SECUNDARY_INDEXES_FILE_PATH, ']', 'utf-8');

        const ret1 = await findInMainIndexFile(15);
        const ret2 = await findInSecundaryFile('Vibook');
        console.log('Pesquisa arquivo de índices principal: ', ret1);
        console.log('Pesquisa arquivo de índices secundario: ', ret2);

        const ret3 = await findInMemoryIndex('12/24/18');
        const ret4 = await findInMemoryIndex2(ContentRating['Teen']);
        console.log('Pesquisa arquivo de índices em memória: ', ret3);
        console.log('Pesquisa arquivo de índices em memória 2: ', ret4);

        //PERGUNTA
        const appName = 'PowerSwitch';
        console.log(`PERGUNTA: Qual o contato de quem criou o app chamado ${appName}?`);
        const answer: RowData = await findInSecundaryFile(appName);
        console.log(`RESPOSTA: `, answer['Developer Email']);
    });

function binarySearch<T extends HasKey>(arr: T[], target: number): T | null {
    let left = 0;
    let right = arr.length - 1;

    while (left <= right) {
        const mid = Math.floor((left + right) / 2);

        if (arr[mid].key === target) {
            return arr[mid];
        } else if (Number(arr[mid].key) < target) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return null;
}


async function findInMainIndexFile(targetRow: number): Promise<RowData> {
    return new Promise((resolve, reject) => {
        //lê arquivo de índices e cria objetos  
        const dataFromIndex = fs.readFileSync(INDEXES_FILE_PATH, 'utf-8');
        const indexObjs: Index[] = JSON.parse(dataFromIndex);

        //procura no arquivo de índices usando a pesquisa binária
        const indexTarget = binarySearch(indexObjs, targetRow);
        if (!indexTarget) {
            reject('Índice inválido.');
            return;
        }

        //pesquisa no arquivo binário do byte inicial do target até + 1000 e cria um objeto 
        const rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: indexTarget.pos, end: indexTarget.pos + LINE_SIZE });

        let ret: RowData;
        rBinFile
            .on('data', (chunk) => {
                ret = JSON.parse(chunk.toString());
            })
            .on('end', () => {
                rBinFile.close();
                resolve(ret);
            })
    });
}

function findInSecundaryFile(targetAppName: string): Promise<RowData> {
    return new Promise((resolve, reject) => {
        //lê arquivo de índices secundario e cria objetos  
        const dataFromSecundaryIndex = fs.readFileSync(SECUNDARY_INDEXES_FILE_PATH, 'utf-8');
        const secundaryIndexObjs: Index[] = JSON.parse(dataFromSecundaryIndex);

        //lê arquivo de índices principal e cria objetos  
        const dataFromIndex = fs.readFileSync(INDEXES_FILE_PATH, 'utf-8');
        const indexObjs: Index[] = JSON.parse(dataFromIndex);

        //procura objeto no arquivo de índices secundário pelo nome
        const target = secundaryIndexObjs.find(index => index.key == targetAppName);
        if (!target) {
            reject('Índice inválido.');
            return;
        }

        //procura objeto no arquivo de índices principal pelo "pos" usando a pesquisa binária 
        const secundaryIndexTarget = binarySearch(indexObjs, target.pos);
        if (!secundaryIndexTarget) {
            reject('Índice inválido.');
            return;
        }

        //pesquisa no arquivo binário do byte inicial do target até + 1000 e cria um objeto 
        const rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: secundaryIndexTarget.pos, end: secundaryIndexTarget.pos + LINE_SIZE });

        let ret: RowData;
        rBinFile
            .on('data', (chunk) => {
                ret = JSON.parse(chunk.toString());
            })
            .on('end', () => {
                rBinFile.close();
                resolve(ret);
            });
    });
}


async function findInMemoryIndex(target: string): Promise<RowData[]> {
    //procura quais registros no índice em memória tem o valor passado por parâmetro de pesquisa
    const targets = inMemoryIndex.filter((index) => index.key == target);

    //para cada target, procura no arquivo binário do byte inicial até o + 1000 e retorna uma promise
    const promises: Promise<RowData>[] = targets.map(target => {
        return new Promise((resolve, reject) => {
            const rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: target.pos, end: target.pos + LINE_SIZE });
            let ret: RowData;
            rBinFile
                .on('data', (chunk) => {
                    ret = JSON.parse(chunk.toString());
                })
                .on('end', () => {
                    rBinFile.close();
                    resolve(ret);
                });
        });
    });

    //concorrentemente resolve todas promises e retorna os objetos
    return Promise.all(promises);
}

function insertInMemoryIndex2(value: Index) {
    const obj: Tree = { key: value.key, pos: value.pos, parents: [], children: [] };

    inMemoryIndex2.push(obj);

    //adiciona como filho/pai de acordo com o valor da chave para cada objeto
    inMemoryIndex2.forEach(reg => {
        const regKeyVal = ContentRating[reg.key as keyof typeof ContentRating];
        const valueKeyVal = ContentRating[obj.key as keyof typeof ContentRating];
        if (regKeyVal > valueKeyVal) {
            reg.children.push(obj);
            obj.parents.push(reg);
        } else if (regKeyVal < valueKeyVal) {
            reg.parents.push(obj);
            obj.children.push(reg);
        }
    });
}


function findInMemoryIndex2(target: ContentRating): Promise<RowData> | null {
    let found: Tree | null = null;

    const find = (targets: Tree[]) => {
        //se o ContentRating do target for igual ao ContentRating da iteração atual retorna
        //se for maior chama a função recursivamente para os pais e se for menor para os filhos
        for (let i = 0; i < targets.length; i++) {
            const keyVal = ContentRating[targets[i].key as keyof typeof ContentRating];

            if (target == keyVal) {
                found = targets[i];
                break;
            } else if (target > keyVal) {
                find(targets[i].parents);
            } else if (target < keyVal) {
                find(targets[i].children);
            }
        }
    }

    find(inMemoryIndex2);
    
    //pesquisa no arquivo binário do byte inicial do target até + 1000 e retorna uma Promise com o objeto

    return new Promise((resolve, reject) => {
        if(!found) reject('Índices em memoria 2: índice não encontrado');
        const idx = found as Index;
        const rBinFile = fs.createReadStream(BIN_FILE_PATH, { start: idx.pos, end: idx.pos + LINE_SIZE });
        let ret: RowData;
        rBinFile
            .on('data', (chunk) => {
                ret = JSON.parse(chunk.toString());
            })
            .on('end', () => {
                rBinFile.close();
                resolve(ret);
            });
    });

}
