const { chromium } = require("playwright")
const path = require("path")

const file = path.resolve(__dirname, "../public/costos-v2-drawer-preview.html")
const out = path.resolve(__dirname, "../tmp/costos-v2-drawer-shots")

;(async () => {
  const browser = await chromium.launch()
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } })
  const cases = [
    ["johnnie", "johnnie-walker"],
    ["chandelle", "chandelle-nestle"],
    ["incomplete", "incomplete-tax-context"],
  ]
  for (const [key, name] of cases) {
    const url = "file:///" + file.replace(/\\/g, "/") + "?f=" + key
    await page.goto(url)
    await page.waitForTimeout(400)
    await page.screenshot({
      path: path.join(out, name + ".png"),
      fullPage: false,
    })
    console.log("saved", name)
  }
  await browser.close()
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
