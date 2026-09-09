---
layout: home

hero:
  name: pinkmask
  text: Keep the shape, lose the secrets.
  tagline: Deterministic SQLite anonymization and subsetting. Copy a real database, share a fake one — identical every run.
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
  - icon: 🔁
    title: Deterministic by design
    details: Same salt and seed, same output — byte for byte, run after run. Fixtures stop drifting and flaky tests stay fixed.
    link: /guide/getting-started
    linkText: How seeding works
  - icon: 🪓
    title: Graph-aware subsetting
    details: Pick root rows with a WHERE clause and a limit. pinkmask walks the foreign-key graph and brings every dependent row along — referential integrity guaranteed.
    link: /config/
    linkText: Subset configuration
  - icon: 🧬
    title: Schema preserved
    details: Tables, views, indexes and triggers are copied in dependency order. The masked database opens, joins, and runs like the original.
    link: /guide/getting-started
    linkText: Schema handling
  - icon: ⚡
    title: Fast Go plugins
    details: Ship custom transformers as Go plugins. The core binary stays lean; heavy catalogs live in .so files loaded on demand.
    link: /plugins/
    linkText: Write a plugin
---

<div class="lp-flow">
  <img src="/mask-flow.svg" alt="pinkmask copies users from input.sqlite to output.sqlite, redacting emails and SSNs while keeping every row" loading="eager">
</div>

<div class="lp-section">
  <h2 class="lp-heading">One command. <span class="redact">Twelve queries.</span> Done.</h2>
  <p class="lp-lead">Point pinkmask at a live SQLite file and a <code>mask.yml</code>. It copies the schema in dependency order, transforms every configured column, and writes a database you can commit, ship, or hand to a contractor.</p>
</div>

<div class="lp-steps">
  <div class="lp-step">
    <div class="lp-step-num">01</div>
    <h3>Describe what to hide</h3>
    <p>Declarative YAML — one entry per column. Hmac, tokenize, faker, regex, null, map.</p>
    <div class="vp-doc"><div class="language-yaml"><pre><code>email:
  type: HmacSha256
  maxlen: 24
ssn:
  type: SetNull
full_name:
  type: FakerName</code></pre></div></div>
  </div>
  <div class="lp-step">
    <div class="lp-step-num">02</div>
    <h3>Copy, masked</h3>
    <p>One command in, one safe file out. Same shape, same row counts, zero secrets.</p>
    <div class="vp-doc"><div class="language-bash"><pre><code>$ pinkmask copy \
    --in input.sqlite \
    --out output.sqlite \
    --config mask.yml \
    --salt "abc" --seed 1</code></pre></div></div>
  </div>
  <div class="lp-step">
    <div class="lp-step-num">03</div>
    <h3>Or take a sample</h3>
    <p>Need 50 US users and everything they ever ordered? Subset along the FK graph.</p>
    <div class="vp-doc"><div class="language-yaml"><pre><code>subset:
  roots:
    - table: users
      where: country = 'US'
      limit: 50</code></pre></div></div>
  </div>
</div>

<div class="lp-section lp-cta">
  <h2 class="lp-heading">Ship it to a PocketBase, too.</h2>
  <p class="lp-lead">The same <code>mask.yml</code>, salt, and seed push masked data straight into a live PocketBase instance — or pull from one. SQLite foreign keys become PocketBase relations and back.</p>
  <div class="vp-doc"><div class="language-bash"><pre><code>$ pinkmask copy \
    --in input.sqlite --out pb:https://masked.example \
    --config mask.yml --salt "abc" --seed 1</code></pre></div></div>
  <a class="lp-button" href="guide/getting-started.html">Get started →</a>
</div>

<div class="dyne-signature">
  <a href="https://dyne.org" rel="noopener" target="_blank">
    <img src="/dyne-logo.svg" alt="Dyne.org free software foundry" width="48" height="48">
  </a>
  <span>Part of the <a href="https://dyne.org" rel="noopener" target="_blank">Dyne.org free software foundry</a>.</span>
</div>
