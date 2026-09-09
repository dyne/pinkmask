import { execFileSync } from 'node:child_process'
import { cpSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { join, resolve } from 'node:path'

const repositoryRoot = resolve(import.meta.dirname, '..')
const docsRoot = join(repositoryRoot, 'docs')
const outputRoot = join(docsRoot, '.vitepress', 'dist')
const versions = releaseTags()
const versionEnvironment = {
  ...process.env,
  PINKMASK_DOC_VERSIONS: JSON.stringify(versions),
}

rmSync(outputRoot, { recursive: true, force: true })
build(docsRoot, outputRoot, versionEnvironment)

// Archived copies must live inside the workspace so bare imports in
// docs/.vitepress/config.ts resolve up the node_modules ancestor chain.
mkdirSync(join(repositoryRoot, 'node_modules', '.tmp-docs'), { recursive: true })
for (const version of versions) {
  const archivedDocs = mkdtempSync(
    join(repositoryRoot, 'node_modules', '.tmp-docs', `${version}-`),
  )
  try {
    extractDocs(version, archivedDocs)
    cpSync(join(docsRoot, '.vitepress'), join(archivedDocs, '.vitepress'), {
      recursive: true,
      filter: (source) =>
        !source.includes(`${join('.vitepress', 'dist')}`) &&
        !source.includes(`${join('.vitepress', 'cache')}`),
    })
    build(archivedDocs, join(outputRoot, 'v', version), {
      ...versionEnvironment,
      PINKMASK_DOCS_VERSION: version,
      VITE_PINKMASK_DOCS_VERSION: version,
    })
  } finally {
    rmSync(archivedDocs, { recursive: true, force: true })
  }
}

rmSync(join(repositoryRoot, 'node_modules', '.tmp-docs'), {
  recursive: true,
  force: true,
})

function releaseTags() {
  const output = execFileSync(
    'git',
    ['tag', '--list', 'v*', '--sort=-version:refname'],
    { cwd: repositoryRoot, encoding: 'utf8' },
  )
  return output
    .trim()
    .split('\n')
    .filter((tag) => /^v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(tag))
    .filter((tag) => hasDocs(tag))
}

function hasDocs(tag) {
  try {
    execFileSync('git', ['cat-file', '-e', `${tag}:docs/index.md`], {
      cwd: repositoryRoot,
      stdio: 'ignore',
    })
    return true
  } catch {
    return false
  }
}

function extractDocs(tag, destination) {
  const archive = execFileSync('git', ['archive', tag, 'docs'], {
    cwd: repositoryRoot,
  })
  execFileSync('tar', ['-x', '--strip-components=1', '-C', destination], {
    cwd: repositoryRoot,
    input: archive,
    stdio: ['pipe', 'ignore', 'inherit'],
  })
}

function build(root, output, environment) {
  console.log(`Building ${environment.PINKMASK_DOCS_VERSION ?? 'latest'} docs -> ${output}`)
  execFileSync('npm', ['exec', '--', 'vitepress', 'build', root, '--outDir', output], {
    cwd: repositoryRoot,
    env: environment,
    stdio: 'inherit',
  })
}