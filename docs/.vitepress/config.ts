import { defineConfig } from 'vitepress'
import type { DefaultTheme } from 'vitepress/theme'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const SITE_ORIGIN = 'https://dyne.github.io'
const SITE_BASE = '/pinkmask/'
const version = process.env.PINKMASK_DOCS_VERSION || null
const versions = parseVersions(process.env.PINKMASK_DOC_VERSIONS)
const base = version ? `${SITE_BASE}v/${version}/` : SITE_BASE

function parseVersions(raw: string | undefined): string[] {
  if (!raw) return []
  try {
    const parsed: unknown = JSON.parse(raw)
    return Array.isArray(parsed)
      ? parsed.filter((entry): entry is string => typeof entry === 'string')
      : []
  } catch {
    return []
  }
}

const DOCS_ROOT = fileURLToPath(new URL('../', import.meta.url))

type SidebarItem = DefaultTheme.SidebarItem
type NavItem = DefaultTheme.NavItem

function pageExists(path: string): boolean {
  if (/^https?:/.test(path)) return true
  // Cross-build links (site root, versioned copies) resolve at deploy time.
  if (path.startsWith(SITE_BASE)) return true
  if (!path.startsWith('/')) return true
  const target = `${DOCS_ROOT}${path.slice(1)}`
  return existsSync(`${target}.md`) || existsSync(`${target}index.md`)
}

function keepSidebar(items: SidebarItem[]): SidebarItem[] {
  return items.flatMap((item) => {
    if (item.items) {
      const kept = keepSidebar(item.items)
      return kept.length ? [{ ...item, items: kept }] : []
    }
    return item.link && pageExists(item.link) ? [item] : []
  })
}


// Absolute URLs bypass VitePress's withBase rewriting, which would double the
// build base when a versioned copy links across builds.
function versionLink(tag: string) {
  return { link: `${SITE_ORIGIN}${SITE_BASE}v/${tag}/`, target: '_self' }
}

const nav: NavItem[] = [
  { text: 'Guide', link: '/guide/getting-started' },
  { text: 'Config', link: '/config/' },
  { text: 'Plugins', link: '/plugins/' },
  ...(versions.length > 0
    ? [{
        text: 'Versions',
        items: [
          {
            text: 'Latest',
            link: `${SITE_ORIGIN}${SITE_BASE}`,
            target: '_self' as const,
          },
          ...versions.map((tag) => ({ text: tag, ...versionLink(tag) })),
        ],
      }]
    : []),
  { text: 'dyne.org', link: 'https://dyne.org' },
]

const sidebar: SidebarItem[] = [
  { text: 'Guide', items: [
    { text: 'Getting started', link: '/guide/getting-started' },
    { text: 'Commands', link: '/guide/commands' },
    { text: 'Schema handling', link: '/guide/schema' },
    { text: 'Subsetting', link: '/guide/subsetting' },
    { text: 'Limitations', link: '/guide/limitations' },
  ]},
  { text: 'Config', items: [
    { text: 'mask.yml reference', link: '/config/' },
    { text: 'Transformers', link: '/config/transformers' },
  ]},
  { text: 'Plugins', items: [
    { text: 'Writing a transformer', link: '/plugins/' },
  ]},
]

export default defineConfig({
  base,
  title: 'pinkmask',
  description: 'Deterministic SQLite anonymization and subsetting.',
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: `${SITE_BASE}favicon.svg` }],
    ['meta', { property: 'og:image', content: `${SITE_ORIGIN}${SITE_BASE}og.png` }],
  ],
  appearance: 'force-dark',
  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'pinkmask',
    nav,
    sidebar: keepSidebar(sidebar),
    socialLinks: [{ icon: 'github', link: 'https://github.com/dyne/pinkmask' }],
    footer: { message: 'Idea and architecture: Puria Nafisi Azizi. AI-assisted development.', copyright: 'Dyne.org foundation' },
    search: { provider: 'local' },
  },
})