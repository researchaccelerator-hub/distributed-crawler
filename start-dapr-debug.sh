#!/bin/bash
# Check for required tools
if ! command -v dlv &> /dev/null; then
    echo "Delve debugger not found. Installing..."
    go install github.com/go-delve/delve/cmd/dlv@latest

    # Add Go bin to PATH temporarily for this session
    export PATH=$PATH:$(go env GOPATH)/bin

    if ! command -v dlv &> /dev/null; then
        echo "ERROR: Failed to install Delve. Please install manually with:"
        echo "    go install github.com/go-delve/delve/cmd/dlv@latest"
        echo "And ensure $(go env GOPATH)/bin is in your PATH by adding to your shell config:"
        echo ""
        echo "For bash: echo 'export PATH=\$PATH:\$(go env GOPATH)/bin' >> ~/.bashrc"
        echo "For zsh:  echo 'export PATH=\$PATH:\$(go env GOPATH)/bin' >> ~/.zshrc"
        echo ""
        echo "Then run: source ~/.zshrc (or ~/.bashrc)"
        exit 1
    fi
fi

if ! command -v dapr &> /dev/null; then
    echo "ERROR: Dapr CLI not found. Please install it first."
    echo "Visit https://docs.dapr.io/getting-started/install-dapr-cli/"
    exit 1
fi

echo "Building application with debug symbols..."
go build -gcflags "all=-N -l" -o ./bin/app

echo "Starting Delve debugger..."
# Pass your app arguments after the double dash
dlv exec ./bin/app --headless --listen=:2345 --api-version=2 --accept-multiclient -- --dapr --dapr-port 6481 --dapr-mode standalone --urls "pokraslampas, Uh3i5E8imbA1NmQy, artgallery, kartiny_arteriart, nW3z1jIUEJ04MmRi, aTqa9vDiYtAxNzZl, i_am_artis, profile_80k, kartiny_muzei_zhivopis, stickeri_tg, maslennikovliga, litvintm, RANBIRROYOFFICE, zubarefff, bloodysx, nevzorovtv, bigpencil, moscowtinderr, guseinnews, exilemusic13, realgoats_channel, findolucky, Q5YX4RslpXpjYzcy, okx_racer_official_announcement, AnuragxCricket, Lucky_Draw_Master, vpXUX8ohCHZhMGRi, -l9Gwg5_7fwxNDcy, livenews_1win, Ton_AI_News, vmb7GohTFco0MzZi, librarypdf1, audiobo0ok, Brasil_Livros_Canal, flibusta_anglysky, enn85, TTTpT, podchi, ManhwaEfficient, oopdf, startups, venture, elguazzar66, raiding_party, ikniga, roxtalk, ishchi_bor_kerak_toshkent, fanton_nft_en, iplteamz4, tonappss, ewdifh, cd4cd, AAAAAEfAkeKlY2M0_yzGyg, eestekhdam_com, SRWORLDShankarBellubbiSir, qiuzhi_zhaopinwang, job4fresherss, jobs4ksa, openkerja, Ae-GkYZFx7lkZmVk, ghostdrive_web3, manjmy, free_edu, CoursesForY0u, moe_obrazovanie, Coursevania, AAAAAFdxBDqPv7ZzVoUASw, sharewood_ws, UCupones, UCupones, hamster_kombat_chat_2, blumcrypto, majors, tapswapai, empirex, memeficlub, seedupdates, notcoin, Cats_housewtf, theYescoin, rWvF3yimCnZmZjI1, Munasarmaxamedcabdi, zd_d7, CIA_cd0, DR_C8, shegongkuE, shegongkurtxin, chadanglie178, chadanglie178, tianyantytcdx, cgplugin, aleytemplatecanva, promodel_3dsky, 4bq_uct8aBtlMzcy, AETemp, graviton24, FGIcdp9T5wQxZGVi, wtfont, photosed, oOX4brwqY10zZmIy, sarkari, Trade_Clup, pumpcoinbets, finansist_busines, MINISH_SAFE, DO_L4, FOREX_SCHOOL_OF_TRADES01, metatrader5signalfx1, sberbank, AlfaBank, Catia_Announcement, iraqedu, mathsbygaganpratap, rojgaarwithankit, utkarshclasses, mlazemna, RankersGurukulLive, exphub910, hamclasixii, RBE_S, BrfLK-3nWcNlNDQy, faktiru, istoriya, test_dot, toplesofficial, hitrosti_hozyaushki, AAAAAE1SEgydlTYwtpT8Ig, facta4ch, facta4ch, Noozhachannel, astrogks, astroobere, DoaaNevisie, abosaedmolaviiii, nikolasferreira, nudaznaki, hashemi2055, i-P6zrhJT8dlOTli, Y7G7jvvRtkk4OGIy, goro_kirill, gosuslugiforparents, mamaolia_family, kids_razvitie, sofia_stuzhuk_official, Ayollar_Juftim_halolim_onalar, ota_onam, razv_deti, MyHomeKidss, vikadmitrieva_psiholog, hf878E9jpa05MzIy, QBJtwNE7IpI3NTgy, goldapple, lupOyKdfTpE0MTBl, pandabuyfind, ZkaaiAghVXxjNzRi, mixit_mood, odezhda_stil_moda, anyclass_faceonline, anH0z15Jd7VlODYy, VA5R96v8jDGOoPXP, token_1win, Kulinariya_retsept, vakansiya_keremi_ishlar_kerak, NRZYjwzHl_NmMTZi, tspyaterochka, AAAAAFczbli2UqYdQsHZ0g, Vt1bAWcdEV43NjYy, culinary_abuse, dx7Lzu0Pgfw3YjVi, xE4tYkSzJAZiZWQy, bums_official, true_caller, JoinRoko, grafunmeme, theZencoin, cybers, game_doc, EAFC_PRIME, mir_supercell, staffonlygame, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, BTcDbr93tCU0NjRi, AAAAAFk7LcSkwO0KCxUBEg, B9dN7jF9FH44NDQy, n6G6CVGwGZtlYWJi, oDQkIwXM0ipiMTcy, isabell_official, fAj8RPaVCqYzNWJi, drzanashooyi, dilya_asliddinovna_dietolog, nox2j8BQ5qJkMzQy, leoday, Mozkhrfats, memachh, HokimBuva_Xokim_Buva, whalememes, ShutkaU, Prikollar_kulgi_xazillar, why4ch, topor, evo_memy, instagramEstory, wefollow_channel, capcutemplates1, jenskiydom, llllil, vidstor, prank_rasmlarr, fotodlyapranka1, visualtop, page_froshi1, pik_ru, AAAAAFDLO3isWhuHliH7mg, domeoru, 3IcZibkv65QxNTBi, AAAAAEMyYVaEVqf6qMUEAQ, Byb93UgFhq4yNWIy, gk_fsk, AAAAAEnehdflhXE2rVX_7w, gHE47kPXgIcyZjFi, DHkOyxqbNPw3OGMy, yuristlar_maslahati, ISyRrvSptAwwODUy, SBUkr, huquqiyaxborot, 7_Ox9vA4oms0NjA6, 7RZ5x9mhncNiYjVi, nk0yA5IYA5MwOTky, TarasOdessa, NetGulagu, tebekasamuel, priness22, EnglishForYou, NimishaMam, NF_English1N, nn_48, farshbuf, lexicona, azglish, ei_tc, fuckingenglish, linkdoni_sib, klientvsprav, trends, trendsetter, FamilyBots, aaaredmarketing, ryanrun, trendach, setlangnie, media_lenta, DrTel18, 2wNydf4venRmZTFi, LearnEng01, no1_oone, Tabobatlar, salomatlik_uz, apteka_tv, magerya, TIBBIYOT_TV, Dorilar_Plus, ahangify, viperrinternational, melodio, Taronalar_qoshiqlar_mp3lar, ReMiXeRapY, Mus_Ton, musicaeel, boscoomuzz, musicir, egorkreed, VDK8qHBzKyevEjFR, h3atJD8y12EzOTZi, B746KyAtAh1iM2Yy, 8EkizoJF_TpjNmYy, koshkii_kotiki, Ni9KhP0qu94yNjhi, Saoo2xkYQVowMmFi, AAAAAEVZR6NQqIVytzIEJQ, NationGeographic, I2-B4Pd4ThBhOGRi, pezeshkian_com, cheatkott, sOj9iDAtUkMyYWQy, novosti_efir, moscowach, rian_ru, INSIDERR_POLITIC, novosti_voinaa, mash, akhbarefori, R7com, temki_zvezdy, nKnnn, stikery_emodzi_memy, temki_yazykii, SuratdagiXaqiqat_yangiliklar, anime_oboi2, wallpaper_4k3d, prrofile_purple, presety, yurasumy, Sara_Xabarlar, times_ukraina, RKadyrov_95, RVvoenkor, AAAAAFCg99bpFf62A_f3yA, jairbolsonarobrasil, lachentyt, ASupersharij, medvedev_telegram, Proxy_Qavi, fasngon, YYYYD, zrzzzz, YYYXY, NNNZN, ZT_EL, LinkdoniCactus, club_digrajsinghrajput, poslanyheart, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, +sX1Ht4p33nFjZDE1, +7rHGiXC8TnsxN2I9, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, TikTokModCloud, tiktokupdatez, AssemBlogger, EasyAPK, irproxy, MBMods, TraidSoft, Mixrootmods, ttskarl, Whatsapp_OB, Sport_HUB_football, Perspolisirfans, Football_Nigeria, proxymtprotoir, EsteghlalFane, Naija_Sports, varzesh3, Esteghlallinews, Perspolis, Match_TV, TelegramTips, telegram, durov, ProxyMTProto, trendingapps, iMTProto, iRoProxy, Myporoxy, caps, darkproxy, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, iPapkorn, iPapkornBots, kino_hd2, kino_filmyc, movie, primefliixx, AAAAAEoGsC-ov5Y0ftVUEQ, kinopoisk, DisneyMasry, XWcyP8XkkNk4NGI1, veronikastepanova20011, mahe_raz, psychology_F, tajvizAngize, kristina_egiazarova14, Sepblog, Recipes_Roma, sexology_vasilenko, neirografica_formagiclife, psyhologiya_mysly, technomotel, naebnet, exploitex, perplexity, media1337, GPTMainNews, whackdoor, Vpn_Shield, TrendWatching24, aipost, PobedaAirlines, aslan_best_blog, QaaCSgt7N9s2MGFi, fpcrussia, vandroukiru, lookatmyway, viktorianskiy_shtil, aeroflot_official, XnqsMz-2LB0wNmIy, pc9yUEhco4Q0OGUy" --max-comments 1 --crawl-id test50 --tdlib-database-url http://tomb218.sg-host.com/tdlib.tgz --min-post-date 2024-01-01 --max-posts 500 --max-depth 1 &
DLV_PID=$!

# Give Delve a moment to initialize
sleep 2

echo "Starting Dapr sidecar..."
echo "Connect your IDE debugger to localhost:2345 now!"
echo "Press Ctrl+C to stop both Dapr and Delve"

# Start Dapr sidecar (this will run in foreground)
# Note: When using dlv, the args are passed to dlv instead of here
dapr run \
    --app-id distributed-scheduler \
    --app-port 6481 \
    --dapr-http-port 3500 \
    --dapr-grpc-port 50001 \
    --log-level debug \
    --app-protocol grpc \
    --resources-path ./resources

# When Dapr exits (Ctrl+C), also kill Delve
echo "Shutting down Delve debugger..."
kill $DLV_PID 2>/dev/null || true
