import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const publicDir = path.join(root, 'public');

const indexPath = path.join(publicDir, 'index.html');
const cssPath = path.join(publicDir, 'styles.css');
const jsPath = path.join(publicDir, 'app.js');
const dataPath = path.join(publicDir, 'data.json');
const outPath = path.join(publicDir, 'standalone.html');

for (const p of [indexPath, cssPath, jsPath, dataPath]) {
  if (!fs.existsSync(p)) {
    console.error(`Missing required file: ${p}`);
    process.exit(1);
  }
}

let html = fs.readFileSync(indexPath, 'utf8');
const css = fs.readFileSync(cssPath, 'utf8');
const js = fs.readFileSync(jsPath, 'utf8');
const data = fs.readFileSync(dataPath, 'utf8');

html = html.replace('<link rel="stylesheet" href="./styles.css" />', `<style>\n${css}\n</style>`);
html = html.replace('<script src="./app.js"></script>', `<script>window.__LIVE_DATA__ = ${data};</script>\n<script>\n${js}\n</script>`);

fs.writeFileSync(outPath, html, 'utf8');
console.log(`Built standalone page: ${outPath}`);
