import logging
import requests
import json
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import emoji
import datetime
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider

DEX_CONTRACT = "cxa0af3165c08318e988cb30993b3048335b94af6c"
nid = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
EXA = 10 ** 18
USDCEXA = 10 ** 6

# Enable logging
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

count = 20865


# Baln Price command#

def finprice(update, context):
    finbnusdpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPrice") \
        .params({"_id": "31"}) \
        .build()
    finbnusdpriceresult = nid.call(finbnusdpricecall)

    finbnusdindec = int(finbnusdpriceresult, 16)
    # convert to icx
    finbnusdfloatindec = float(finbnusdindec)
    finbnusdconverted = finbnusdfloatindec / EXA
    # make it 3 decimals
    finbnusdprice = str("%.4f" % finbnusdconverted)
    finpricetext = "FIN Price: " + finbnusdprice + " bnUSD"
    update.message.reply_text(finpricetext)
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def gbetprice(update, context):
    gbetbnusdpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "GBET/bnUSD"}) \
        .build()
    gbetbnusdpriceresult = nid.call(gbetbnusdpricecall)

    gbetbnusdindec = int(gbetbnusdpriceresult, 16)
    # convert to icx
    gbetbnusdfloatindec = float(gbetbnusdindec)
    gbetbnusdconverted = gbetbnusdfloatindec / EXA
    # make it 3 decimals
    gbetbnusdprice = str("%.4f" % gbetbnusdconverted)
    gbetpricetext = "Gbet Price: " + gbetbnusdprice + " bnUSD"
    update.message.reply_text(gbetpricetext)
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def cftprice(update, context):
    cftsicxpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "CFT/sICX"}) \
        .build()
    cftsicxpriceresult = nid.call(cftsicxpricecall)

    cftsicxindec = int(cftsicxpriceresult, 16)
    # convert to icx
    cftsicxfloatindec = float(cftsicxindec)
    cftsicxconverted = cftsicxfloatindec / EXA
    cftsicxprice = str("%.4f" % cftsicxconverted)
    cftpricetext = "CFT Price: " + cftsicxprice + " sICX"
    update.message.reply_text(cftpricetext)
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def ommprice(update, context):
    ommusdspricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "OMM/USDS"}) \
        .build()
    ommpriceresult = nid.call(ommusdspricecall)
    # convert hex to dec
    ommusdsindec = int(ommpriceresult, 16)
    # convert to icx
    ommusdsfloatindec = float(ommusdsindec)
    ommusdsconverted = ommusdsfloatindec / EXA
    # make it 3 decimals

    ommusdcpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "OMM/IUSDC"}) \
        .build()

    ommusdcpriceresult = nid.call(ommusdcpricecall)

    hextodec = int(ommusdcpriceresult, 16)
    inttofloat = float(hextodec)
    exaconverted = inttofloat / USDCEXA
    ommusdcprice = str("%.3f" % exaconverted)

    ommsicxpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "OMM/sICX"}) \
        .build()

    ommusdcpriceresult = nid.call(ommsicxpricecall)

    sicxhextodec = int(ommusdcpriceresult, 16)
    sicxinttofloat = float(sicxhextodec)
    sicxconverted = sicxinttofloat / EXA
    ommsicxprice = str("%.3f" % sicxconverted)

    ommprice = str(
        "Omm Price: " + "\n" + "%.3f" % ommusdsconverted + " USDS" + "\n" + ommusdcprice + " IUSDC" + "\n" + ommsicxprice + " sICX")
    update.message.reply_text(ommprice)
    # logging interaction#
    logger.info('Omm price Command sent')
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def balnprice(update, context):
    # price_api = requests.get('https://balanced.rhizome.dev/api/v2/dex/quote/')
    # pricedata = price_api.text
    # price_json = json.loads(pricedata)
    # balnprice = price_json['baln_bnusd_quote']
    # floatbalnprice = float(balnprice)
    # balnicxprice = price_json['baln_sicx_quote']
    # floatbalnicxprice = float(balnicxprice)
    # balnpriceresult = "BALN Price: " + "%.2f" % floatbalnprice + " bnUSD / " + "%.2f" % floatbalnicxprice + " ICX"
    balnbnusdpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "BALN/bnUSD"}) \
        .build()

    balnbnusdpriceresult = nid.call(balnbnusdpricecall)

    balnbnusdhextodec = int(balnbnusdpriceresult, 16)
    balnbnusdfloat = float(balnbnusdhextodec)
    balnbnusdconv = balnbnusdfloat / EXA
    balnbnusdresult = str("%.3f" % balnbnusdconv)

    balnsicxpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPriceByName") \
        .params({"_name": "BALN/sICX"}) \
        .build()

    balnsicxpriceresult = nid.call(balnsicxpricecall)

    balnsicxhextodec = int(balnsicxpriceresult, 16)
    balnsicxfloat = float(balnsicxhextodec)
    balnsicxconv = balnsicxfloat / EXA
    balnsicxresult = str("%.3f" % balnsicxconv)

    update.message.reply_text("Baln Prices: " + "\n" + balnbnusdresult + " bnUSD" + "\n" + balnsicxresult + " sICX")
    # logging interaction#
    logger.info('Balnprice Command sent')
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def fullinfo(update, context):
    ####API####

    # Coingecko#
    cgapi = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=icon&vs_currencies=usd')
    cg_dict = cgapi.json()

    # DIV API#
    div_api = requests.get('https://balanced.rhizome.dev/api/v2/dividends/fees/')
    divdata = div_api.text
    div_json = json.loads(divdata)

    # MCAP DVI#
    mcap_api = requests.get('https://balanced.rhizome.dev/api/v2/baln/market-cap/')
    mcapdata = mcap_api.text
    mcap_json = json.loads(mcapdata)

    # Price Api#
    price_api = requests.get('https://balanced.rhizome.dev/api/v2/dex/quote/')
    pricedata = price_api.text
    price_json = json.loads(pricedata)

    # Token API#
    token_api = requests.get('https://balanced.rhizome.dev/api/v2/baln/supply/')
    tokendata = token_api.text
    token_json = json.loads(tokendata)

    # APY API#
    apy_api = requests.get('https://balanced.rhizome.dev/api/v2/rewards/apy/')
    apydata = apy_api.text
    apy_json = json.loads(apydata)

    # Pool API#
    pool_api = requests.get('https://balanced.rhizome.dev/api/v2/pool/')
    pooldata = pool_api.text
    pool_json = json.loads(pooldata)

    # Total TVL API#
    tvl_api = requests.get('https://balanced.rhizome.dev/api/v2/tvl/')
    tvldata = tvl_api.text
    tvl_json = json.loads(tvldata)

    # loan TVL API#
    loantvl_api = requests.get('https://balanced.rhizome.dev/api/v2/tvl/loans/')
    loantvldata = loantvl_api.text
    loantvl_json = json.loads(loantvldata)

    # DEX TVL API#
    dextvl_api = requests.get('https://balanced.rhizome.dev/api/v2/tvl/dex/')
    dextvldata = dextvl_api.text
    dextvl_json = json.loads(dextvldata)

    # TVLS#
    dextvl = dextvl_json['dex_total_tvl_usd']
    intdextvl = int(float(dextvl))
    convdextvl = (f'{intdextvl:,d}')
    dextvlresult = "DEX TVL: " + convdextvl + " USD"

    loantvl = loantvl_json['loans_tvl_usd']
    intloantvl = int(float(loantvl))
    convloantvl = (f'{intloantvl:,d}')
    loantvlresult = "Loan TVL: " + convloantvl + " USD"

    totaltvl = tvl_json['tvl_usd']
    inttvl = int(float(totaltvl))
    convtvl = (f'{inttvl:,d}')
    tvlresult = "Total TVL: " + convtvl + " USD"

    # Pools#
    BalnPoolbase = pool_json['baln_bnusd_pool']['base']
    BalnPoolquote = pool_json['baln_bnusd_pool']['quote']
    intbalnpoolbase = int(float(BalnPoolbase))
    intBalnPoolquote = int(float(BalnPoolquote))
    convBalnpoolbase = (f'{intbalnpoolbase:,d}')
    convBalnPoolquote = (f'{intBalnPoolquote:,d}')
    balnpoolresult = "Baln/bnUSD Pool: " + convBalnpoolbase + " Baln & " + convBalnPoolquote + " bnUSD"

    balnicxpoolbase = pool_json['baln_sicx_pool']['base']
    BalnicxPoolquote = pool_json['baln_sicx_pool']['quote']
    intbalnicxpoolbase = int(float(balnicxpoolbase))
    intBalnicxPoolquote = int(float(BalnicxPoolquote))
    convBalnicxpoolbase = (f'{intbalnicxpoolbase:,d}')
    convBalnicxPoolquote = (f'{intBalnicxPoolquote:,d}')
    balnicxpoolresult = "Baln/ICX Pool: " + convBalnicxpoolbase + " Baln & " + convBalnicxPoolquote + " ICX"

    icxsicxpool = pool_json['sicx_icx_pool']['quote']
    inticxsicxpool = int(float(icxsicxpool))
    convicxsicxpool = (f'{inticxsicxpool:,d}')
    icxpoolresult = "ICX Pool: " + convicxsicxpool + " ICX"

    bnusdpoolbase = pool_json['sicx_bnusd_pool']['base']
    bnusdpoolquote = pool_json['sicx_bnusd_pool']['quote']
    intbnusdpoolbase = int(float(bnusdpoolbase))
    intbnusdpoolquote = int(float(bnusdpoolquote))
    convbnusdpoolbase = (f'{intbnusdpoolbase:,d}')
    convbnusdpoolquote = (f'{intbnusdpoolquote:,d}')
    bnusdpoolresult = "bnUSD Pool: " + convbnusdpoolbase + " SICX & " + convbnusdpoolquote + " bnUSD"

    # divs#

    sicx_fees = div_json['sicx_fees']
    intsicx_fees = int(float(sicx_fees))
    convsicx_fees = (f'{intsicx_fees:,d}')
    sicx_feesresult = "Amount of ICX fees: " + convsicx_fees + " ICX"

    bnusd_fees = div_json['bnusd_fees']
    intsbnusd_fees = int(float(bnusd_fees))
    convbnusd_fees = (f'{intsbnusd_fees:,d}')
    bnusd_feesresult = "Amount of bnUSD fees: " + convbnusd_fees + " bnUSD"

    baln_fees = div_json['baln_fees']
    intsbaln_fees = int(float(baln_fees))
    convbaln_fees = (f'{intsbaln_fees:,d}')
    baln_feesresult = "Amount of Baln fees: " + convbaln_fees + " Baln"

    totaldivusd = div_json['total_fees_usd']
    inttotaldivusd = int(float(totaldivusd))
    convtotaldivusd = (f'{inttotaldivusd:,d}')
    totaldivusdresult = "Total amount of fees: " + convtotaldivusd + " USD"

    # PRICE#

    mcap = mcap_json['baln_market_cap']
    intmcap = int(float(mcap))
    convmcap = (f'{intmcap:,d}')
    mcapresult = "Baln Market cap: " + convmcap + " USD"

    balnprice = price_json['baln_bnusd_quote']
    floatbalnprice = float(balnprice)
    balnicxprice = price_json['baln_sicx_quote']
    floatbalnicxprice = float(balnicxprice)
    balnpriceresult = "BALN Price: " + "%.2f" % floatbalnprice + " bnUSD / " + "%.2f" % floatbalnicxprice + " ICX"

    sicxprice = price_json['sicx_bnusd_quote']
    floatsicxprice = float(sicxprice)
    sicxpriceresult = "sICX Price: " + "%.2f" % floatsicxprice + " bnUSD"

    # Balanced token#

    totalbaln = token_json['baln_total_supply']
    inttotalpool = int(float(totalbaln))
    convtotalpool = (f'{inttotalpool:,d}')
    totalbalnresult = "Current total supply: " + convtotalpool + " Baln"

    stakedbaln = token_json['baln_staked_supply']
    intstakedbaln = int(float(stakedbaln))
    convstakedbaln = (f'{intstakedbaln:,d}')
    stakedbalnresult = "Current staked supply: " + convstakedbaln + " Baln"

    # APY#

    strsicxAPY = apy_json['sicx_icx_apy']
    strbalnAPY = apy_json['baln_bnusd_apy']
    strbnusdAPY = apy_json['sicx_bnusd_apy']
    strborrowAPY = apy_json['bnusd_borrow_apy']

    borrowAPY = float(strborrowAPY)
    sicxAPY = float(strsicxAPY)
    balnAPY = float(strbalnAPY)
    bnusdAPY = float(strbnusdAPY)

    APYresult = "Current Loan APY: " + "%.2f" % borrowAPY + "\n" + "Current sicx pool APY: " + "%.2f" % sicxAPY + " %" + "\n" + "Current Baln pool APY: " + "%.2f" % balnAPY + " %" + "\n" + "Current bnUSD pool APY: " + "%.2f" % bnusdAPY + " %"

    # Pricediff#

    price1 = cg_dict.get('icon')
    price2 = float(price1.get('usd'))
    result = "ICX Coingecko Price: " + str(price2) + " USD"

    resultsicxprice = 'SICX/BNUSD Price: ' + "%.2f" % floatsicxprice + " bnUSD"
    superresult = result + '\n' + resultsicxprice

    value = float("%.2f" % floatsicxprice)
    percent_diff = ((price2 - value) / value) * 100
    pricediffresult = superresult + '\n' + 'Difference in percentage is : ' + "%.2f" % percent_diff + '%'

    # TEXTS#
    tokentext = (emoji.emojize(':coin:')) + "---Balanced Token---" + (
        emoji.emojize(':coin:')) + "\n" + totalbalnresult + "\n" + stakedbalnresult + "\n" + "\n"
    apytext = (emoji.emojize(':farmer:')) + "---Current APYs---" + (emoji.emojize(':farmer:')) + "\n" + APYresult
    tvltext = (emoji.emojize(':classical_building:')) + "---Total Value Locked---" + (
        emoji.emojize(':classical_building:')) + "\n" + dextvlresult + "\n" + loantvlresult + "\n" + tvlresult
    pooltext = (emoji.emojize(':droplet:')) + "---Pools---" + (emoji.emojize(
        ':droplet:')) + "\n" + balnpoolresult + "\n" + balnicxpoolresult + "\n" + icxpoolresult + "\n" + bnusdpoolresult + "\n"
    divtext = (emoji.emojize(':ticket:')) + "---Fees---" + (emoji.emojize(
        ':ticket:')) + "\n" + sicx_feesresult + "\n" + bnusd_feesresult + "\n" + baln_feesresult + "\n" + totaldivusdresult + "\n"
    pricetext = (emoji.emojize(':money_with_wings:')) + "---Prices---" + (emoji.emojize(
        ':money_with_wings:')) + "\n" + mcapresult + "\n" + balnpriceresult + "\n" + sicxpriceresult + "\n"
    text = "***Balanced info ***" + "\n" + "\n" + tvltext + "\n" + "\n" + pricetext + "\n" + pooltext + "\n" + tokentext + apytext + "\n" + "\n" + divtext + "\n" + (
        emoji.emojize(':bar_chart:')) + "---Arbitrage---" + (emoji.emojize(':bar_chart:')) + "\n" + pricediffresult

    update.message.reply_text(text)
    # logging interaction#
    logger.info('fullinfo Command sent')
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def info(update, context):
    update.message.reply_text(
        'This bot is created by @Sazern and uses Brian li´s api https://balanced.rhizome.dev/docs#/ to fetch data from the balanced protocol')
    # logging interaction#
    logger.info('info Command sent')
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


# Get amount of bot interaction#
def counter(update, context):
    global count
    resultcount = "current amount of interactions with bot: " + str(count)
    update.message.reply_text(resultcount)


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)
    global count
    count = count + 1
    logger.info(f'{count} amount of interactions ')


def main():
    """Start the bot."""


# Create the Updater and pass it your bot's token.
# Make sure to set use_context=True to use the new context based callbacks
# Post version 12 this will no longer be necessary
updater = Updater("INSERT_TG_TOKEN_HERE", use_context=True)
# Get the dispatcher to register handlers
dp = updater.dispatcher

# on different commands - answer in Telegram
dp.add_handler(CommandHandler("balnprice", balnprice))
dp.add_handler(CommandHandler("ommprice", ommprice))
dp.add_handler(CommandHandler("cftprice", cftprice))
dp.add_handler(CommandHandler("gbetprice", gbetprice))
dp.add_handler(CommandHandler("finprice", finprice))
dp.add_handler(CommandHandler("info", info))
dp.add_handler(CommandHandler("fullinfo", fullinfo))
dp.add_handler(CommandHandler("counter", counter))

# log all errors
dp.add_error_handler(error)

# Start the Bot
updater.start_polling()

# Run the bot until you press Ctrl-C or the process receives SIGINT,
# SIGTERM or SIGABRT. This should be used most of the time, since
# start_polling() is non-blocking and will stop the bot gracefully.
updater.idle()

if __name__ == '__main__':
    main()