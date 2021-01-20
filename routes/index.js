const fs = require('fs').promises
const path = require('path')
const express = require('express')
const router = express.Router()
const Pool = require('pg').Pool
const dbUser = require('../secrets/postgresql_db_user.json')
const pool = new Pool(dbUser)

const query = (q, p = []) => new Promise((resolve, reject) => {
  pool.query(q, p, (err, data) => err ? reject(err) : resolve(data))
})

let processInfo = {
  status: 'idle',
  errors: []
}

/* GET init-csv-data page. */
router.get('/init-csv-data', async (req, res, next) => {
  if (processInfo.status !== 'idle') return res.json({ data: 'PROCESS ALREADY RUNNING' })
  try {

    // Check if csv-data folder is ok
    processInfo.status = 'checking'
    const csvDataDirPath = path.join(__dirname, '../csv-data')
    const rawDirs = await fs.readdir(csvDataDirPath)
    const dirs = rawDirs.filter(dir => !dir.match(/^\./))
    if (dirs.length !== 4
        || dirs.indexOf('0_caracteristiques') === -1
        || dirs.indexOf('1_lieux') === -1
        || dirs.indexOf('2_vehicules') === -1
        || dirs.indexOf('3_usagers') === -1) throw `DIRECTORY SHOULD CONTAIN: 0_caracteristiques/, 1_lieux/, 2_vehicules/, 3_usagers/ IN: ${csvDataDirPath}`
    for (const dir of dirs) {
      const dirPath = path.join(csvDataDirPath, dir)
      const dirStat = await fs.lstat(dirPath)
      const dirIsDirectory = dirStat.isDirectory()
      if (!dirIsDirectory) throw `NOT A DIRECTORY: ${dirPath}`
    }

    // Clear database
    processInfo.status = 'clearing'
    const checkE = await query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_type='BASE TABLE'
      AND table_schema='public'
      AND table_catalog='${dbUser.database}'
      AND table_name='events'`)
    const checkV = await query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_type='BASE TABLE'
      AND table_schema='public'
      AND table_catalog='${dbUser.database}'
      AND table_name='vehicles'`)
    const checkU = await query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_type='BASE TABLE'
      AND table_schema='public'
      AND table_catalog='${dbUser.database}'
      AND table_name='users'`)

    if (checkE.rows.length > 0) await query('DROP TABLE events')
    if (checkV.rows.length > 0) await query('DROP TABLE vehicles')
    if (checkU.rows.length > 0) await query('DROP TABLE users')
    
    const createdE = await query(`CREATE TABLE events (
      _id SERIAL PRIMARY KEY, num_acc TEXT, an TEXT, mois TEXT,
      jour TEXT, hrmn TEXT, lum TEXT, agg TEXT, int TEXT,
      atm TEXT, col TEXT, com TEXT, adr TEXT, gps TEXT,
      lat TEXT, long TEXT, dep TEXT, catr TEXT, voie TEXT,
      v1 TEXT, v2 TEXT, circ TEXT, nbv TEXT, pr TEXT, pr1 TEXT,
      vosp TEXT, prof TEXT, plan TEXT, lartpc TEXT, larrout TEXT,
      surf TEXT, infra TEXT, situ TEXT, env1 TEXT, vma TEXT
    )`)
    const createdV = await query(`CREATE TABLE vehicles (
      _id SERIAL PRIMARY KEY, id_vehicule TEXT, num_veh TEXT,
      num_acc TEXT, senc TEXT, catv TEXT, occutc TEXT, obs TEXT,
      obsm TEXT, choc TEXT, manv TEXT, motor TEXT
    )`)
    const createdU = await query(`CREATE TABLE users (
      _id SERIAL PRIMARY KEY, id_vehicule TEXT, num_veh TEXT,
      num_acc TEXT, place TEXT, catu TEXT, grav TEXT, sexe TEXT,
      an_nais TEXT, trajet TEXT, secu TEXT, secu1 TEXT, secu2 TEXT,
      secu3 TEXT, locp TEXT, actp TEXT, etatp TEXT
    )`)

    // Save data
    processInfo.status = 'saving'
    const caracteristiquesFilesPath = path.join(csvDataDirPath, '0_caracteristiques')
    const lieuxFilesPath = path.join(csvDataDirPath, '1_lieux')
    const vehiculesFilesPath = path.join(csvDataDirPath, '2_vehicules')
    const usagersFilesPath = path.join(csvDataDirPath, '3_usagers')
    const savedC = await saveType(caracteristiquesFilesPath, 'characteristics')
    const savedL = await saveType(lieuxFilesPath, 'places')
    const savedV = await saveType(vehiculesFilesPath, 'vehicles')
    const savedU = await saveType(usagersFilesPath, 'users')
    
    processInfo.status = 'idle'
    return res.json({ data: 'PROCESS STARTED' })
  } catch (err) {
    console.log(err)
    return res.json({ err })
  }
})

async function saveType (dirPath, type) {
  console.log(type.toUpperCase())
  const files = await fs.readdir(dirPath)
  const filesToRead = []
  for (file of files) {
    const filePath = path.join(dirPath, file)
    const fileStat = await fs.lstat(filePath)
    const fileIsCsv = fileStat.isFile() && filePath.match(/.csv$/)
    if (fileIsCsv) filesToRead.push(filePath)
  }
  for (filePath of filesToRead) {
    processInfo.status = `saving file ${filePath}`
    const processedFile = await saveFile(filePath, type)
  }
  return true
}

async function saveFile (filePath, type) {
  const fileContent = await fs.readFile(filePath, 'utf-8')
  const rawRows = fileContent
    .replace(/\r/gm, '')
    .replace(/"/gm, '')
    .replace(/;/gm, ',')
    .replace(/\t/gm, ',')
    .split('\n')
  const rawTable = rawRows.map(row => row.split(','))
  const head = rawTable[0].map(e => e.toLowerCase())
  const table = rawTable.slice(1)
  const nbLinesPerChunk = 200
  const nbChunks = Math.ceil(table.length / nbLinesPerChunk)
  const tableChunks = new Array(nbChunks)
    .fill(null)
    .map((e, i) => table.slice(i * nbLinesPerChunk, (i + 1) * nbLinesPerChunk))
  console.log(head.length, filePath)
  for (const chunk of tableChunks) {
    const chunkSaved = await saveChunk(type, chunk, head)
  }
  return true
}

async function saveChunk (type, chunk, head) {
  console.log(type, chunk.length, head.join(','))
  switch (type) {
    case 'characteristics':
      const headList = head.join(',')
      const values = chunk.map((line, lineNb) => {
        return `(${line.map((cell, i) => `$${1 + i + head.length * lineNb}`)})`
      })
      const q = `INSERT INTO events (${headList}) VALUES ${values.join(',')}`
      const p = []
      chunk.forEach(line => line.forEach(cell => cell === '' ? p.push('NULL') : p.push(cell)))
      console.log(q)
      console.log(p)
      console.log('\n\n\n')
      const result = await query(q, p)
      console.log(result)
    return true
    case 'places':
    return true
    case 'vehicles':
    return true
    case 'users':
    return true
    default:
    return true
  }

}

module.exports = router
