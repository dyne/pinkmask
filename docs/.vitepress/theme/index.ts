import DefaultTheme from 'vitepress/theme'
import './custom.css'
import Layout from './Layout.vue'
import './versions.css'

export default {
  ...DefaultTheme,
  Layout,
}