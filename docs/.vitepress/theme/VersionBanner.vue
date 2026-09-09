<script setup lang="ts">
const version = import.meta.env.VITE_PINKMASK_DOCS_VERSION
</script>

<template>
  <div v-if="version" class="version-banner" role="status">
    You are reading the documentation for <strong>{{ version }}</strong>.
    <a href="/pinkmask/">View the latest documentation.</a>
  </div>
</template>
