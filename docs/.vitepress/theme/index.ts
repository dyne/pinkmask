import DefaultTheme from 'vitepress/theme'
import '@fontsource/archivo-black/400.css'
import '@fontsource/fira-code/400.css'
import '@fontsource/fira-code/500.css'
import '@fontsource/fira-code/600.css'
import './custom.css'
import Layout from './Layout.vue'
import './versions.css'

export default {
  ...DefaultTheme,
  Layout,
}