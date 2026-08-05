const fs = require('fs');
const path = require('path');

// Keep local .env support dependency-free; values already exported by the shell win.
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, 'utf8');
    for (const line of content.split(/\r?\n/)) {
        const match = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
        if (!match) {
            continue;
        }

        const key = match[1];
        let value = match[2];
        const trimmedValue = value.trim();
        if ((trimmedValue.startsWith('"') && trimmedValue.endsWith('"')) || (trimmedValue.startsWith("'") && trimmedValue.endsWith("'"))) {
            value = trimmedValue.slice(1, -1);
        } else {
            value = value.replace(/#.*$/, '').trim();
        }

        if (process.env[key] === undefined) {
            process.env[key] = value;
        }
    }
}
