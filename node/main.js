const puppeteer = require('puppeteer');

async function scrapeData() {
    // Launch the browser in the new headless mode
    const browser = await puppeteer.launch({ headless: "new" });
    const page = await browser.newPage();

    // Navigate to the page
    await page.goto('https://www.google.com');

    // Get the title of the page
    const title = await page.title();
    console.log(`The title of the page is: ${title}`);

    // Close the browser
    await browser.close();
}

scrapeData().catch(console.error);
