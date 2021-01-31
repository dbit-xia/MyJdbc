/*
 * @Author: yululiang
 * @Date: 2019-08-06 10:16:59
 * @Last Modified by: yululiang
 * @Last Modified time: 2019-08-06 12:37:48
 */

 /**
  * 下载jar包
  */
const fs = require('fs');
const path = require('path');
const Thenjs = require('thenjs');
const axios = require('axios');
const jarNames = [
    'commons-codec-1.12.jar',
    'commons-collections4-4.3.jar',
    'commons-compress-1.18.jar',
    'commons-math3-3.6.1.jar',
    'curvesapi-1.06.jar',
    'fastjson-1.2.58.jar',
    'hutool-core-4.6.1.jar',
    'hutool-log-4.6.1.jar',
    'hutool-poi-4.6.1.jar',
    'poi-4.1.0.jar',
    'poi-ooxml-4.1.0.jar',
    'poi-ooxml-schemas-4.1.0.jar',
    'protobuf-java-3.6.1.jar',
    'xlsx-streamer-2.1.0.jar',
    'xmlbeans-3.1.0.jar',
    'minimal-json-0.9.5.jar',
    'sqlite-jdbc-3.7.2.jar'
];
const jarRoot = __dirname + '/java/libs';
const sourceUrl = 'http://gitlab.runsasoft.com:8999/cdn/jars/raw/master/';

function downloadJars() {
    let fns = [];

    if (!fs.existsSync(jarRoot)) {
        fs.mkdirSync(jarRoot);
    }

    for (let jarName of jarNames) {
        fns.push((cont) => {
            let fullname = path.join(jarRoot, jarName);
            if (fs.existsSync(fullname)) {
                console.log('文件已存在:' + fullname);
                return cont();
            }

            let stream = fs.createWriteStream(fullname);

            console.log(sourceUrl + jarName);

            axios({
                method: 'get',
                url: sourceUrl + jarName,
                responseType: 'stream'
            }).then(response=>{
                response.data.pipe(stream)
                    .on('finish',
                        res => {
                            console.log(jarName + '下载成功');
                            return cont();
                        }
                    );
            })
        });
    }

    Thenjs.series(fns).fin((c, err, result) => {
        console.log('下载结束');
    });
}

downloadJars();
