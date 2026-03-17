#!/usr/bin/env node
import { backfillOverrideLogFromExecutions } from './behavioral-accountability-utils.mjs';

const result = backfillOverrideLogFromExecutions();
console.log(JSON.stringify(result, null, 2));
