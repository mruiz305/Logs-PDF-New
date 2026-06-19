const path = require('path');
const projectRoot = path.join(__dirname, '..');

require('dotenv').config({ path: path.join(projectRoot, '.env') });
require('dotenv').config({ path: path.join(projectRoot, 'env') });
