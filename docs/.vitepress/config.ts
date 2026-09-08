import { defineConfig } from 'vitepress'

export default defineConfig({
  base: '/pinkmask/',
  title: 'pinkmask',
  description: 'Deterministic SQLite anonymization and subsetting.',
  head: [
    ['link', { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }],
    ['meta', { property: 'og:image', content: 'https://dyne.github.io/pinkmask/og.png' }],
  ],
  appearance: 'force-dark',
  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'pinkmask',
    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'Config', link: '/config/' },
      { text: 'Plugins', link: '/plugins/' },
      { text: 'dyne.org', link: 'https://dyne.org' },
    ],
    sidebar: [
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
    ],
    socialLinks: [{ icon: 'github', link: 'https://github.com/dyne/pinkmask' }],
    footer: { message: 'Idea and architecture: Puria Nafisi Azizi. AI-assisted development.', copyright: 'Dyne.org foundation' },
    search: { provider: 'local' },
  },
})
