---
layout: home

hero:
  name: pinkmask
  text: Keep the shape, lose the secrets.
  tagline: Deterministic SQLite anonymization and subsetting. Inspired by Greenmask.
  image:
    src: /logo.svg
    alt: pinkmask
  actions:
    - theme: brand
      text: Get started
      link: /guide/getting-started
    - theme: alt
      text: mask.yml reference
      link: /config/
    - theme: alt
      text: GitHub
      link: https://github.com/dyne/pinkmask

features:
  - title: Deterministic
    details: Same salt and seed, same output. Every run produces the identical anonymized database, so fixtures and tests stay stable.
  - title: Graph-aware subsetting
    details: Pick root rows with a WHERE clause and a limit; pinkmask follows foreign keys to keep referential integrity intact.
  - title: Schema preserved
    details: Tables, views, indexes and triggers are copied in dependency order. The output opens like the original.
  - title: Fast plugins
    details: Ship custom transformers as Go plugins. The core binary stays lean; heavy catalogs live in .so files.
---

<div class="dyne-signature">
  <a href="https://dyne.org" rel="noopener" target="_blank">
    <img src="/dyne-logo.svg" alt="Dyne.org free software foundry" width="48" height="48">
  </a>
  <span>Part of the <a href="https://dyne.org" rel="noopener" target="_blank">Dyne.org free software foundry</a>.</span>
</div>
