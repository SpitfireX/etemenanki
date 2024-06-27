#[allow(dead_code)]
mod common {
    use std::cmp::min;

    use etemenanki::Datastore;
    use libcl_rs::Corpus;
    use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng};

    pub fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    pub fn setup_rand(len: usize, max: usize) -> Vec<usize> {
        let rng = rng();
        let dist = Uniform::new(0, max);
        Vec::from_iter(dist.sample_iter(rng).take(len))
    }

    /// segments the range into windows that are randomly distributed throughout 0..max
    /// the sum of the window spans equals total, plus/minus one maximum window size
    pub fn setup_windows(total: usize, max: usize, wmin: usize, wmax: usize) -> Vec<(usize, usize)> {
        let mut rng = rng();
        let sdist = Uniform::new(0, max);
        let wdist = Uniform::new(wmin, wmax);
        let mut windows: Vec<(usize, usize)> = Vec::new();
        let mut sum = 0;

        while sum < total {
            let start = sdist.sample(&mut rng);
            let end = min(max, start + wdist.sample(&mut rng));
            windows.push((start, end));
            sum += end - start;
        }

        windows
    }

    pub fn setup_jumps(total: usize, max: usize, maxjumps: usize, jumplen: isize) -> Vec<usize> {
        let mut rng = rng();
        let sdist = Uniform::new(0, max);
        let ndist = Uniform::new(0, maxjumps);
        let odist = Uniform::new(-jumplen, jumplen);
        let mut series = Vec::new();
        let mut sum = 0;

        while sum < total {
            let start = sdist.sample(&mut rng);
            let mut jumps = vec![start];
            for _ in 0..ndist.sample(&mut rng) {
                let offset = odist.sample(&mut rng);
                jumps.push((start as isize + offset).clamp(0, max as isize) as usize)
            }
            sum += jumps.len();
            series.push(jumps);
        }

        series.sort_unstable_by_key(|v| *v.first().unwrap());
        series.into_iter().flatten().collect()
    }

    pub static mut DATASTORE_NAME: &'static str = "ziggurat";

    pub fn open_ziggurat() -> Datastore<'static> {
        // open ziggurat datastore
        Datastore::open(unsafe { DATASTORE_NAME }).unwrap()
    }

    pub fn open_cwb() -> Corpus {
        // open CWB corpus
        Corpus::new("cwb/registry", "encow_cwb").expect("Could not open corpus")
    }

    pub static REGEX_TESTS: [&'static str; 22] = [
        r"ziggurat",
        r"be.+",
        r"imp.ss.ble",
        r"colou?r",
        r"...+able",
        r"(work|works|worked|working)",
        r"super.+listic.+ous",
        r"show(s|ed|n|ing)?",
        r"(.*lier|.*liest)",
        r".*(lier|liest)",
        r".*li(er|est)",
        r".*lie(r|st)",
        r".*(rr.+rr|ss.+ss|tt.+tt).*y",
        r"(un)?easy",
        r".{3,}(ness(es)?|it(y|ies)|(tion|ment)s?)",
        r".+tio.+",
        r".*a{3,}.*",
        r"[^!-~]+",
        r"([aeiou][bcdfghjklmnpqrstvwxyz]{2}){4,}",
        r"([aeiou][b-df-hj-np-tv-z]{2}){4,}",
        r"\p{Lu}{6,10}",
        r"\p{Arabic}+طة",
    ];

    // total: 143640813
    pub const MIXED_TYPES: [&'static str; 11] = ["the", "end", "is", "near", "Cthulhu", "will", "rise", "and", "destroy", "every", "ziggurat"];

    // total: 456835502
    pub const TOP_TYPES: [&'static str; 10] = [",", "the", ".", "of", "to", "and", "a", "in", "that", "is", ];

    // types of freq within 10_000..50_000
    // total: 735659
    pub const MEDFREQ_TYPES: [&'static str; 100] = ["applause", "Weber", "gentlemen", "Lesbian", "Romeo", "LD", "Territory", "Juliet", "coupons", "Laundry", "peasants", "Castro", "(?)", "coursework", "reconsider", "Alexandria", "treasury", "congress", "Fukushima", "chess", "1874", "imperialism", "Quest", "NPR", "republican", "Claudia", "provinces", "Counsel", "ordinance", "Sophia", "consolidated", "Humboldt", "SPORTS", "Wake", "two-thirds", "cane", "USSR", "creek", "prohibition", "comrades", "Cantor", "Somalia", "Peters", "settlers", "Ago", "injunction", "stain", "prevail", "propositions", "repetition", "Rand", "Notre", "reactor", "disposition", "taxed", "coincidence", "Levi", "gasoline", "canada", "Osama", "sued", "Lakers", "statutes", "hereby", "Surveillance", "JAMES", "heroic", "declares", "MEP", "Debates", "suspend", "levied", "drone", "decree", "Peterson", "Academics", "levy", "LAW", "Lean", "notwithstanding", "guardian", "unconstitutional", "dam", "Judicial", "playground", "supra", "exercised", "yarn", "Marco", "territorial", "remark", "Babylon", "Guns", "Crane", "Cannon", "commissioners", "Promise", "accord", "12:20", "Held", ];

    // types of freq within 5..50
    // total: 23955
    pub const LOWFREQ_TYPES: [&'static str; 1000] = ["concupiscible", "Atv", "synderesis", "libdbi", "LeFer", "a-signifying", "ASLI", "WhidbeyAIR", "Jadid", "Pedreros", "Warrum", "timtyler", "ChessWorld", "Insam", "🎩", "JOLLEY", "Ramirezes", "Coray", "ANAP", "Heybourne", "DHCS", "Schlesselman", "Huxman", "MAUGHAN", "gwern", "LERATA", "VUWLR", "Enigmerald", "HEYBOURNE", "Corfman", "Bisconti", "coupon-holder", "Ogigami", "gtk_box_pack_start", "MacFoy", "McClennen", "Sassa", "DEILD", "noro", "Matkins", "Grechko", "libdv", "Raveson", "Browndog", "Agt", "jdkchem", "julite", "U.S.G.P.O.", "Tamboer", "bluepie", "VQA", "libisoburn", "libburn", "DOCAS", "LetMeStartBySaying", "Sayoko", "Supersonics", "Grindler", "CETF", "Navassa", "McPeak", "Brookover", "Crennel", "Juab", "Movits", "Bhangmeter", "26.05.2014", "Xev", "Kotraba", "Bellringer", "DecalBatch", "ColumbiaFAVS", "LEAKER", "Viciously", "COPRAC", "Haigwood", "County-based", "ingerirsi", "tingo", "Inconsolata", "Representative(s)", "HSTW", "Lennox-Gastaut", "Cambron", "Chumina", "SNEP", "EDAC", "11.09.2013", "Bargnani", "Raaf", "Dalbandin", "GRANMA", "Haymet", "Tingo", "damascene", "-1.3.6", "ltm", "OcTEApi", "FiXato", "Zawiyah", "Sandin", "PEACHES", "DemNoMore", "SEQRA", "NoIframe", "lfs.org", "Anaxyrus", "Ronilson", "Rent-a-Cat", "LEARFIELD", "관악산", "Remastering", "PICS~~~", "ANDJULIET", "NEEEEEED", "UOS", "AUI-S", "michael_vassar", "GRAMA", "Tom_McCabe", "pedanterrific", "Tangē", "Maddaford", "McGairey", "lessdazed", "145030", "145115", "BannedbytheGuardian", "ECUPL", "5735", "Dewese", "detinue", "Sullinger", "avy.com", "Trapnall", "Mentona", "Zebrowski", "Macrolife", "R+J", "Kendl", "Nevarez", "carsont", "Seymore", "Lingis", "Sacredness", "Gushee", "KDFX", "Mado", "Citizen-Lawyer", "Pela", "Silverwind", "Benfer", "Feurer", "SuperSonics", "FEPC", "McFrazier", "Gierek", "WESTLAW", "726.00", "Zandri", "WPPT", "Mantrid", "Lamya", "theduchessofkitty", "Cathouse", "gint", "IOIO", "HERMENEUTICS", "LASTORIA", "netbsd", "libpthread", "Grinols", "kochi-muso", "Lynah", "D-Huntington", "j_kies", "Elcano", "Dvorkovich", "circumscriptions", "group-orientedness", "ingerenza", "DangerousThing", "Obelleiro", "ACWA", "SSMW", "Wabanaki", "08.09.2013", "Magueijo", "lxiv", "Focchi", "Palissimo", "Traumdeutung", "Damos", "Tractenberg", "Américo", "opvolger", "postmedieval", "12.09.2013", "anticommunism", "13.09.2013", "Lause", "libjte", "libbz", "Eilatan", "ain.sh", "Buy-back", "goLOCAL", "Enlists", "KWPA", "Madaxweyne", "Siiw", "YearOldest", "Iftaar", "KJZZ.org", "Wüst", "rwu", "KFL", "willar", "son.com", "19.08.2013", "Chair(s)", "MWt", "RideLondon", "McConnaissance", "Robrt", "LAShortsFest", "ARCLIGHT", "erDweet", "Sukenick", "fria", "everafter", "2896762", "guillermo", "Hvac", "sumary", "MDCL", "CORAY", "Juwana", "Woolfolk", "shminux", "02:42:3", "Marcic", "folkdom", "Loebe", "melodi", "319.364.1580", "Tamminator", "Nornagest", "Kenshu", "Betse", "sergeant-at-arms", "McGurrin", "libcwd", "EMSA", "Rattly", "Knodl", "Tauchen", "Ripp", "Pridemore", "Kestell", "Huebsch", "macheteros", "Kleefisch", "Bies", "jurisidiction", "Ziegelbauer", "Petrowski", "MONCADA", "localepurge", "ISoc", "boreas", "filoque", "NZF", "estimative", "Delevan", "rander", "supermajoritarian", "45,000.00", "lead-crime", "buncombe", "Greenholtz", "HKUL", "force-wide", "hbox", "LEGISLATOR", "AMARO", "MCNEIL", "fuzislippers", "Shigo", "Republicons", "Chaturanga", "canecutters", "textualist", "Boinod", "Expressively", "F.CAS", "Escutia", "Secundae", "stevewhitemd", "group-oriented", "RegistrationAuthorized", "KnowledgeBaseDownloadsProduct", "Gorvy", "Ketai", "ROBESON", "BOOQATAY", "Metaph", "Qoriley", "jaallesiyaad.com", "CEEGAAG", "Caalamka", "PUNTLAND", "nuclearpower", "Muqdisho", "kalshaale", "Buuhoodle", "Furaystay", "Dhagaha", "Caalamkii", "Korodhay", "Levend", "Fatahaadda", "Daadadka", "Qasaaraha", "Kadageen", "Gobood", "Dhawr", "Ugaleen", "Gudaha", "Lasheegay", "Huwanka", "mockturtle", "Ciidamada", "Magacaabay", "Ardaynimadii", "Faarax-Garaad", "taagan", "Bergling", "aalo", "Horyaal", "Caqabadaha", "Noncombatant", "Dibadda", "JAALIYADDA", "DHIGGIISA", "KULMAY", "GALAY", "QAYB", "XORRIYADDA", "MAALINTA", "17.08.2013", "Baarlamaan", "Soomaaliyeed", "Qaranka", "TV-ga", "Daawo", "Muuqaal", "Maqal", "Faallo", "Kaydka", "LTCVT", "pira", "LeeTerryNE", "31.08.2013", "FFVs", "01.09.2013", "eigenlijk", "vond", "04.09.2013", "TeachersFirst.com", "8915", "helemaal", "Focarino", "ICLQ", "15.09.2013", "18.09.2013", "Esrock", "Bibelot", "Manigault", "relators", "CAUSATION", "Ten-Day", "concupiscent", "3.21.14", "Cristalla", "Frankenberg", "overfulfilled", "gidley", "ght.com", "hatterasli", "ngs.com", "Classicgreeti", "als.com", "Carolinan", "Unsell", "jimzinsocal", "Stirman", "Obion", "Asberry", "Curtsinger", "Ables", "92515", "Venetis", "Francione", "stomatologists", "NickMom", "Unrepresented", "Pro-lifers", "Dorticos", "agricultural-livestock", "Cannady", "Senator(s)", "Zwonitzer", "Diad", "ferus", "misr", "ægypt", "ägypten", "aegyptos", "indiscrete", "masr", "dpkg-query", "ian.org", "Brunnis", "cloop", "penological", "Luhmannian", "domicil", "Songjiang", "SoCA", "co-implicated", "Kurdistantribune.com", "Judería", "GASOLINE", "CHAMPLIN", "REFINING", "Degn", "LEMMON", "libburnia", "aclo", "cal.m", "MCDANIEL", "forms-based", "GABLER", "HEDDA", "Tubbiolo", "equiX", "equixtrack", "-9370", "-8819", "Mudéjar", "Swithenbank", "KEARNS", "Kambhampati", "GCET", "OneClick", "10505", "WBTB", "UOF", "video-tape", "Guiterman", "cat-woman", "D-North", "Liukkonen", "KLAFTER", "Sprigman", "Raustiala", "Glendo", "Ireen", "whathaveyou", "KGOV", "Moonlite", "GoodBadInsightful", "Mihrab", "Gabelli", "long-absent", "myRWU", "Chaosmosis", "Frameline", "inter-mountain", "Multiyear", "Truini", "$package", "Andover-Harvard", "CDAC", "undebatable", "Maciak", "Library-wide", "Transphobic", "Outfest", "LaVictoire", "04:57:2", "08:43:2", "Señores", "introducción", "Reynos", "post-singularity", "09:24:0", "ENGAGES", "Petryk", "Nerison", "Tranel", "Marklein", "Ballweg", "Honadel", "Klenke", "Kaufert", "Kerkman", "Litjens", "Kapenga", "Endsley", "Knilans", "Spanbauer", "libgdx", "Papagiannis", "Monsen", "disfranchise", "nonalined", "Faurot", "Calitics", "Freada", "Negroids", "Veene", "Spewing", "girardi", "Plff", "ク", "グ", "の発音について", "Tonkotsu", "yunwoo", "特定商取引法に基づく表記", "Moribund", "Ruthe", "Catolico", "Text-A-Tip", "Peevey", "Dout", "Leedeth", "Roxo", "POINDEXTER", "to-wit", "Disbarment", "irrepealable", "Mellott", "frequens", "weeknotes", "OskiCat", "Yorkshire.com", "Hyperlapses", "Mikako", "Voltigeur", "★★½", "Gillespy", "moogle", "Triunvirato", "Gearhardt", "Claassen", "microbrigades", "Food-Blog", "Propaedeutic", "NoteSake", "NoteMesh", "Lernu", "VoteHemp.com", "blameworthiness", "SideJobTrack", "ReusableBags.com", "PocketMod", "PatientsLikeMe.com", "Inter-Vac", "Home-Exchange", "HabitForge", "CatalogChoice", "Brainology", "Kuroshi", "Projjex", "ABEbooks.com", "Goishi", "Samarkand.net", "uliGo", "9,802", "GOPM", "malanga", "19,433", "Pandora.com", "US-origin", "Live-Radio", "wet-shaving", "HTGAM", "Filmcritic.com", "Charco", "Bullgoose", "Fendrihan", "Muehle-Pinsel", "RazoRock", "CHET", "Harkrider", "Shavemac", "Shavemyface", "ShavingProfessionals.com", "ShavingStuff.com", "ShavingZone.com", "Shaveshop", "Straight-Razor", "Wet-Shaving", "Wetshavers", "BlogDesk", "scafidi", "FITALY", "Fontifier", "Ottolib", "Scrybe", "WriteToMyBlog", "manzana", "letouryorkshire", "skap", "Lobbs", "Earnie", "CoMo", "humboldtkids", "FAVS", "Tolton", "third-trimester", "NORK", "Gazes", "spatially-referenced", "528,000", "ALLEZ", "HAVENS", "FÊTE", "Kamome", "TMDb", "LDing", "BoostPack", "SmallTalk", "lasersaber", "-6567", "JSConf", "canefields", "parceling", "PhillyGuy", "Jumel", "Hackpad", "wrong-doer", "scop", "eack", "Mutnodjmet", "RMPBS", "SaltForbidding", "InfoTools", "ConsideredProject", "PoliticsHillary", "SportFM", "Fullosseous", "Trochilus", "indorse", "land-office", "Hudley-Hayes", "Melish", "Frappa", "ZeroDivide", "Pailthorp", "stonecutter", "PRESSWIRE", "JeanneG", "Acy", "Valanciunas", "Zavieh", "FRW", "non-attorney", "pgtips", "princegeo", "STREVELL", "PROPOSITIONS", "rge.rcm", "Folkies", "p.ca", "SALUCI", "-8331", "Denesha", "kiteboarding", "BATEMAN", "97.50", "LGSF", "red-shift", "-7953", "CONSTANTS", "06268", "SData", "NSTAC", "STRICKEN", "49:11", "68:5", "OUS.COM", "AMID", "617.5", "Jacot", "LaMorte", "Caryatids", "Near-future", "sheens", "feinstein", "Palco", "Rashall", "Headwrapping", "Watchpaul", "Reporta", "StudioTwoTen", "Wolfananda", "moviedad", "Klamblog", "JohnChiv", "Jendocino", "Highboldtage", "greenwheels", "Maltepe", "Duchamps", "Transsexuality", "Leguin", "high-threat", "KRCB.org", "SUFFRAGE", "94928", "Labath", "most-favored", "NorthBayVoice.org", "ex-liberal", "NRTW", "MCCCD", "Pişkin", "kjzzphoenix", "unsex", "E-Member", "Lincolniana", "Aletta", "AUGMENTATION", "-2081", "NZPA", "CPEC", "Colombie-Britannique", "drogues", "Sensibilisation", "getSize", "iPredict", "romancist", "Enlgish", "Sexe", "ㅠㅠ", "Williamsburg-James", "non-relation", "Infobases", "unwisdom", "PAROLE", "Harsdorf", "08:07:1", "08:11:5", "Kapanke", "01:10:3", "CarlShulman", "10:45:5", "12:26:2", "12:47:1", "03:24:0", "floorperiod", "08:18:2", "04:22:0", "non-fixed", "06:52:0", "01:13:2", "03:51:5", "03:38:4", "07:10:2", "07:23:5", "08:55:1", "handsomeness", "Desrtopa", "10:23:1", "10:26:5", "02:26:2", "Stuart_Armstrong", "10:27:5", "10:40:4", "01:16:2", "09:50:0", "Disciplina", "Honesta", "04:28:1", "12:28:3", "01:36:0", "01:19:2", "02:48:0", "04:00:1", "05:14:3", "05:26:2", "04:06:1", "self-preserving", "pnrjulius", "08:41:3", "StackLife", "Steeleville", "psychosphere", "Sniffin-Marinoff", "Schmelz", "Gragg", "Unrecognition", "Incriminates", "NUDITY", "city-run", "Montserrado", "NEBEKER", "HLSL", "percenter", "Marchmain", "typeglob", "Symdump", "fuz", "1,1,2,3,5,8", "plinky", "fishbot", "WikiHome", "Schendel", "herquack-gottbill", "L.E.S.", "Havelaar", "PAIRED", "11549", "-4391", "Veazie", "0.8.3", "-0.8.1", "pre-cautions", "uSv", "Daini", "AboutLaundry", "July|August", "obstinancy", "USCCAN", "Copjec", "Ayerza", "Pedrero", "MusEditions", "cathouse", "Illustra", "Post-show", "TheologyOnline", "27:59", "Soft-tissue", "Sempervirens", "-7881", "not-there", "GtkWidget", "pack_start", "DPPA", "kubla", "kme", "some-thing", "kantian", "textualism", "Officer-Involved", "lemonwade.com", "realisms", "DANNATT", "JACQUES-ALAIN", "DAUPHIN", "SLAVOJ", "ZIZEK", "attny", "RUBINSTEIN", "self-presence", "non-involved", "non-categorical", "LAPDonline.org", "milod", "aggelia-online", "ebil", "Crinkled", "Linctix", "Caidos", "Sevillano", "Generalife", "Group-Oriented", "HAJI", "Mozarabic", "Fravel", "Shaoqi", "all-fired", "proto-fascists", "first-use", "rwendland", "Krasic", "Kureyon", "ool.m", "libt", "Thornell", "$current", "better-armed", "Murillos", "64:4", "50:5", "IRRIGATION", "Florenz", "50:4", "Stockpot", "paulhaydon", "Newton-Dunn", "misapprehend", "Everyware", "Context-aware", "TopTips", "94:9", "behaviour-based", "ARMing", "Robisch", "Behaviour-based", "Subsumption", "Ganssle", "Onyeador", "cevinius", "Doczizi", "jscottb", "birdmun", "hoff", "autopoeisis", "Biographia", "esemplastic", "Trezevant", "damnum", "absque", "exaptation", "r-Rahman", "lah_shirazi", "Crocco", "engrossment", "Diyar", "mosaiced", "cosmodrome", "Inters", "Ontonagon", "Mahmod", "Peniston", "NeoConScum", "Jianming", "DETERRENCE", "brasse", "Comrs", "Línea", "InCopy", "half-measure", "Terrenoire", "chunat", "Gunservatively", "bobbi", "ALLENDE", "Phaal", "FETE", "Safley", "Palmigiano", "Wolfish", "Procunier", "+R", "lacan.com", "macchina", "Ecrits", "lib.rar", "lexx", "Boringhieri", "Bollati", "origi", "Boabdil", "self-cancellation", "Suspiro", "Mexuar", "portmap", "Hellie", "Repostings", "inconsistence", "super-majorities", "Callcut", "cadenced", "sauvages", "aperto", "SlowClouds", "Charlow", "Ridnour", "narratologists", "Sinhababu", "Erfani", "Gowder", "gnome-dom", "libidinally", "Fliess", "LTSS", "Saussurian", "Brietbart", "ind.exe", "mpif", "nelsoni", "exsul", "59864", ];

    // total = 1000 DUH
    pub const HAPAX_TYPES: [&'static str; 1000] = ["all-responded", "HaZZa", "ACCESSIBLITY", "imtreseted", "R-pi", "praphesor", "ekkkkk", "ude.Heh", "You-Really-Need-To-Know", "TeamArch", "SuryaMukhi", "DWItians", "Codeathon", "LOLer", "MrMindBlow", "teachique", "JorgeLTE", "extra-signifying", "Teraflare", "meaning-for-humans", "IRRO", "bay.SAW", "Midoo", "socday", "wafdi", "qaabilay", "xafiiskiisa", "wasaare", "matter(ing)", "Qaybo", "rogay", "Kusoo", "xaalad", "Shariif", "meta-real", "mater-reality", "delsuional", "avpmom", "mentionated", "chfurlan", "Ganaele", "Gaddafi-controlled", "movie-time", "head-drop", "physical-electronic", "a-signification", "rhawkins", "Diagramming-some", "I=If", "-0750914086", "ACLUCF", "ATSD", "Nuenlist", "Nationally-based", "USMLM", "achievements-decent", "more-came", "people-lots", "NPBC", "includes-and", "has-issues", "Sithoff", "22,1972", "NZLJ", "NZ-US", "legislature-only", "demand-low", "houseofrefuge.org", "streetofdreams.com", "tour-takers", "ARMLS", "consistently-rising", "Polynésienne", "NZACL", "boyfriend-at-the-time", "Attorneys-Social", "Attorneys-Conduct", "Fontham", "Practice-United", "Documents-United", "Correspondence-United", "Briefs-United", "Composition-United", "Familyl", "Missile-deliverable", "BALCA", "151.17.0.0", "Mediation-United", "Enforcement-United", "Law-United", "Legislation-Louisiana", "Relations-United", "Employment-Louisiana", "of-Louisiana", "Crmic-Grotic", "Client-United", "lawsuit-abuse", "Half-integrated", "Plinklet", "space-retrieval", "Ryugyong-Hotel", "prietors", "verwer", "kamiel", "Atlas-sized", "Svenshinhan", "Quiesco", "Trethedj", "mjeh", "tree-falling-in-the-woods", "SiDe", "interivewed", "Notachi", "dad-disappointed-with-a-naughty-child", "counter-barrage", "life-respecting", "Fønss", "Bøndergaard", "Little-TEL", "Myasischevs", "oh-so-authoritatively", "pirollilaw", "coincidence-story", "Pirollilaw", "matters-people", "markob", "genre-bashing", "genre-biased", "hastily-composed", "utopion", "genre-sniffing", "best-embodied", "offered-nobody", "expostory", "Cottage-hopping", "straight-genre", "block-logic", "Passion-another", "vein-and", "Smalltimore", "Freudianly", "IMWINKELRIED", "GIANNELLI", "BIBELOT", "belligarent", "TR-CSR", "Slynx", "Fenkl", "Liuke", "read-without-reading", "19720628", "19720627", "Forostenko", "ngs.m", "19740714", "19740630", "paglinawan", "jeneveve", "Yomari", "fgsfdg", "Arabaci", "kahlood", "Cecci", "crazy_gyal", "Shtank", "Azalie", "stadtmiller", "blrur", "mcerlain", "nazmus", "idnan", "matharu", "bmemi", "henderson-bell", "Anallely", "barzare", "Stromley", "Brimnon", "PicZ", "zakieh", "FARZANA", "AShlee", "pearla", "cfhknbhvkghl", "jdodj", "camillia", "minibrigades", "peffer", "Holldia", "ton-capacity", "otilia", "vivianafreire", "Dimitruk", "Aamnda", "cragin", "maragh", "Syier", "Woodgeard", "denica", "Schnazzy", "taylan", "demarias", "fsron", "Stegemeyer", "Boogerz", "rtggdfg", "Neeca", "YAARA", "dfgadsfgas", "Taquita", "joenisha", "Gleidy", "cent-a-kilowatt-hour", "htryhbgh", "laxmidas", "munvel", "rievra", "roemo", "timebucket", "trumamn", "clarise", "habibur", "Eleshia", "bailah", "modha", "klaz", "sherice", "akintewe", "kannika", "froese", "ceaira", "HJTEGHTYHT", "cponcering", "Critisicisms", "camelle", "Iraq-Sulaimani", "Bakaloree", "ASIGNMENT", "fiction-focused", "cybershelves", "unsound-angry", "Word-when", "Word-analyzing", "Nawroly", "IYLEP", "62732.3", "explains-maybe", "Braegen", "Humphys", "Dijarmett", "Minrod", "McCaCholssam", "Carragan", "Barragen", "Unreadiness", "Stiberfield", "Sutten", "end-conformity", "Vallindnigham", "Cool-Hand", "Tankesley", "Truet", "Tilison", "Stenarth", "Sniedley", "Semell", "ParedesThis", "Pentagrass", "Pearceall", "Parrit", "word-meditate", "Miliner", "Mishow", "Marteny", "JohnsonMy", "Hoffisen", "WilsonThis", "controlled-at", "Perminta", "LuckenbachThis", "Flies-the", "ophen", "Barler", "OlBion", "RobisonMy", "Ashwirth", "Taxible", "dispasal", "951.785.5222", "951.785.2222", "NYC.org", "wag-the-dogs", "weaponless-defense", "SAFE-for", "BigPeace", "PREPARED-know", "attack-an", "authorization-written", "CommuteSmart", "spend-he", "KeepPeaceInStapleton.com", "Vanocur", "IRLC", "hesitant-and", "Kahdaffi", "Montclair-Newark", "Clinton-Jarret-Rice", "anything-ever", "stomatologiests", "overfulfillments", "hypocritacl", "Naceo", "alzadora", "sugarcane-producing", "Jacbson", "mirodams", "Villarenas", "Heinrichs-Wolpert", "rectangulo", "Agramounte", "proficient-level", "262239", "19770727", "19770726", "QUARBERG", "eternization", "BROOKOVER", "focchi", "INDISCRETE", "UNREPEATABLE", "attacks-though", "apophantic", "paul_nijjar", "Joruma", "gpodder", "DUSEMPI", "views_rss_itunes", "1400x1400", "volunes", "picture-assisted", "xmllib", "dérobade", "~return", "contents~", "ASCII-to-double", "tagName_id", "attrName_id", "id-based", "-03798", "Nelson-related", "Medi-Cal-only", "DOMination", "ADHCs", "sense-generating", "Narratologists", "-00193", "Branagh-style", "Evermost", "Olivier-style", "Super-majoritarian", "insfoar", "justfieid", "non-super-majoritarian", "moussakka", "supermajoritan", "enchilada-like", "Ariostian", "GIEREK", "PIOTOR", "JAROSZEWI", "utterability", "KONGA", "lallation", "interpretation-beyond", "Kazzan", "MedSat", "hand-eating", "Cryo-Unit", "Hand-eating", "ingerirmi", "Bio-Viziers", "Cryo-unit", "requests-requests", "Schnik", "lusticon", "profession-called", "8166145", "indiscreta", "Castrovillari", "psicoanalisi", "HKUVPN", "psicoanalitico", "ACEnet", "plus-de-dire", "achondrospasia", "PKReader", "ingerire", "WinFrames", "ingerere", "Borulawski", "11.0.0.446", "HKUESD", "Marynoll", "atDalbandin", "28.827", ",1128", ",1129", ",1133", "63.974", "Dmin=", "onerightguy", "ggbx", "Koblog", "OSHIT", "UglyHonest", "Chomes", "BettinaVLA", "SyriaTweet", "Lady_Penquin", "mouse-hugger", "gskirocks", "reactionariez", "", "President", "Contemplating", "CFLancop", "tnjudy", "yankeemom", "McLeanSix", "MadisonPeale", ",1142", "non-al-Qaeda", "Vienna-summit-to-Cuban-missile-crisis", "therapry", "Wakinekona", "Nimatnama", "oooookk", "RETRIBUTIVISM", "out-doo", "-00205", "neuˈtriːno", "njuːˈtriːnoʊ", "convictions-might", "Brunson-a", "Takeisha", "exingencies", "SODDIT", "FANTASYLAND", "Original-Anon", "stuntmans", "sheepoo", "infcat", "saysomething", "apparamentally", "gawalmandi", "transhistoric", "Kratiam", "Gadbud", "Jashn-e-Baharan", "mystico-scientific", "Minamoto-no-Tametomo", "chpter", "unlocalizable", "ancient-India", "bang-injected", "electoneurobiological", "authorimost", "transfor-mational", "transfor-mation", "Szirko", "Rhogan", "Wautisher", "noergy", "Johnnyqiao", "European-strength", "hausdorff", "codradicts", "Machiste", "musuculature", "Troussen", "acleron", "airline-like", "keys.gnupg.net", "Guffawe", "libcam", "atapicam", "libacl-devel", "libreadline-dev", "iso_write_opts_set_joliet_utf", "health-replacing", "iso_conv_name_chars", "cdrskin", "list_speeds", "check_media", "isoburn_igopt_set_relaxed", "soon-reveals", "isoburn_igopt_joliet_utf", "joliet_utf", "joliet-utf", "bad_outname", "print_outname", "isoburn_conv_name_chars", "libburnia-team", "scdbackup", "AME.age", "Metadatasets", "Coproj", "crosswalked", "Mirwasi", "Bazanai", "focusees", "性本善", "人之初", "hedgehopper", "Hugereplied", "战役学", "demanding-old-man", "第二炮兵战役学", "pedagogically-critical", "structurally-constituted", "CO-editor", "SOKOKU", "Ashiabor", "Congress-full", "collections-with", "regularly-containing", "-94993", "Library-issued", "Collections-which", "3062-26-0", "wcpltn.org", "gazetteer-like", "dreamguide", "BGNs", "loveluciddreams", "3062-34-1", "-893062", "3062-36-8", "Ceeia", "Tyx", "lab-ish", "TimeLess", "Video-taped", "siiw", "Milod", "Novakowsky", "UNLVRebels.com", "Kinnkinick", "deerhide", "cat-beautiful", "theoffocers", "j.r", "non-textualist", "textualism-through", "done-read", "Cat-Woman", "Three-zero", "Venezuela-Iran", "ship[ocean]", "russiannavyblog", "NEWNOWNEXT", "a-gag", "GtkBox", "Temte", "Wyoming-Nebraska", "flexible-yield", "scemes", "multiple-silo-per-bird", "rail-garrison", "DuncePack", "non-decomposed", "Milagruous", "al-Azmi", "659.035", "659.380", "Not-So-Old", "BunnyRanch", "659.358", "3-26-07", "TheologyOnLine", "God.com", "6-15-2006", "Bible-turn", "82,283", "659.330", "659.710", "Esori", "659.225", "659.545", "659.505", "659.175", "399.230", "659.270", "659.340", "659.015", "Avgikos", "WolfeVideo.com", "lesbian-interest", "Revèle", "Bllod", "pairing-symptom", "alikeness", "couple-symtom", "WolfeOnDemand.com", "BuskFilms", "COUNTY-OWNED", "crowd-moving", "metaword", "Indyweek.com", "617.13", "Rensfeldt", "025.000", "word-junk", "soul-throbbing", "create_redirect", "JobSyndicate", "aximatic", "bhungh", "Chilum", "majority-irrespective", "seat-except", "ilk-the", "evildoers-has", "auto-affirm", "gurdjieffian", "multicomponential", "Pignarre", "auntiecarol", "Howbert", "maturana-style", "sickness-causing", "disable-docs", "dbixx", "end-user-visible", "exersion", "Archjr", "riggermortus", "FRELINO", "Mozambians", "Wramblin", "131450", "19771014", "MOSAMBIQUE", "19771013", "frequent-traveler", "Somegood", "oldHP", "mCcaughn", "5,686,986", "57,444", "197,815", "152,758", "76,116", "48,387", "subdistricting", "Yakos", "McCAUGHN", "$blang", "STASHtype", "theHarvard", "Shuchut", "Lemnius", "Bartholmaeus", "Bausner", "Arbitre", "eschevinage", "languages-Arabic", "libraries-Access", "Services-will", "Izms", "Lillvik", "McConaughead", "McConaughey-shaped", "657123", "McConaugheyan", "all-female-alt-country", "m-getting-too-old-for-this-shit", "Mal-fifty-cent", "Mal-efi-cent", "Fight-By-Numbers", "bringing-his-work-home", "legacy-oriented", "HarvardLibrary", "CRINITUS", "midsize-town", "Texasness", "good-time-ness", "Expurgandorum", "Novissimus", "extingished", "possibility-space", "quasi-dominant", "wage-jobs", "Rapture-worried", "Polwarth-Lyell", "Prohibe", "Hanks-ian", "Correspondientes", "non-rom-com", "Jacobinismo", "econblogs", "quatrocientos", "prediction_markets", "perfectly-cheekboned", "unFriendly", "superplagues", "January-er", "prisons-she", "guard-and", "around-almost", "Anaconda-on", "cryto-liberal", "dagnabbitt", "Schlossel", "Shikori", "Minnesota-when", "food-good", "apttitude", "rcritical", "Hansen-mmm", "guitar-and", "in-consumable", "non-autopoieticizable", "posturally", "Plato-Aristotle-Descartes-Spinoza-Leibniz-Kant-Fichte-Schelling-Hegel-Heidegger", "Lukacs-Bloch-Benjamin-Kracauer-Horkheimer-Adorno-Lefebvre-Harvey-Postone", "Gravalosa", "SEPREP", "alBatch", "NanoBrain", "enfatuation", "Dumbbomb", "cross-participant", "object-dynamics", "nordin", "Yaworsky", "Piano-B", "JOINing", "SPLITing", "Granisle", "LISMS", "Aiyansh", "Heinlein-honors", "ConnectNY", "slept-and", "Heinein", "19771030", "Hauthorne", "valerate+Cyproterone", "Bay-or", "cheryl_scholar", "photographer-meaning", "Mexico-that", "daughter-reputedly", "Psikiyatri", "Yazıcı", "Yargıç", "Üçok", "09.475", "39.835", "LATimesworld", "Tükel", "Kulaksızoğlu", "Kimlikler", "Savana-la-Mar", "coodinating", "Sports-produced", "çıkarmama", "tuvalete", "Numayu", "-104.00", "tarihli", "anarcho-transhumanists", "disaster-maps", "dheimstadt", "planets-or", "humans-myths", "dirtworlds", "urine-but", "macrolife-mobile-organism", "pre-fallout", "macrolife", "Farmhams", "Reqib", "Ehmede", "CigerXwin", "Oullies", "Gulf-namely", "Bakuri", "Bumblepuppy", "Apoci", "Rojhilati", "Rojavi", "UWK", "Zaxo", "authorreplied", "LibertyWalk", "WordPressed", "politize", "fi-consider", "KDPi", "favorites-I", "difrences", "behdinis", "soranis", "Shymalen", "Kwan-Ak-San", "Panamory", "Octovia", "housebout", "otheRscott", "Speedpod", "proto-EF", "main-party", "love-looking", "robot-lovers", "life-doing", "CENTJ", "morgagees", "erimitus", "Leicocephlus", "nigerrima", "c-data", "99303", "macro-biota", "short_in", "time_if", "31,556,925.9747", "98934", "time_that", "taken_that", "foilows", "tajii", "e(bar)", "h(bar)", "Polarizable-Vacuum", "c-metric", "drawers_I", "drop-seizures", "Tele-Consultants", "table_that", "appointive_we", "are_I", "frame-invariance", "director-The", "Zuštiak-Palissimo", "Lake_I", "Viňarský", "think_that", "Biot-savart", "sir_that", "question_where", "Convention_if", "actiou", "States_inasmuch", "members_that", "not_that", "then_when", "omendment", "(continuing)_in", "Cruz-Aedo", "attentention", "commitmittee", "(m)psi", "clerks_the", "-11304", "order_that", "psi(m)", "Farakis", "before_a", "Schatzer", "manner_unless", "WARRUM", "moment_two", "Red-Shifted", "blevd", "RClayborn", "time-author", "mychan", "MYLABEL", "CWDEBUG", ":myapplication:", "Leckar", "Shainis", ":debug:", "taste-derived", "constitating", "Galvestion", "1,511,576.17", "tourist-ladened", "ollows", "1789-tha", "srtates", "court-expecially", "momentoun", "coupon-holders", "Lamorut", "bank-notes-a", "16,264", "274,895", "Garayta", "Salup", "SWASTIKing", "Vidaurreta", "Santrayll", "him-Dewhurst", "DewHurts", "19741023", "CONSTIT", "19741103", "jedid", "salāh", "al-jadīd", "Duwayr", "Attasi", "tedcruz", "DavidHDewhurst", "couponholder", "VolunteerTV", "Shoukhat", "Guanatanmo", "then-Pakistani", "PolicyMap", "GeoScienceWorld", "non-empsyched", "unbarterable", "Hecceity", "Hongisto", "21,035,377.15", "bryngelsson", "Correlationist", "Rentaneko", "TumblrMoreDiggPrintEmail", "Asmik", "INSTRUCTOR-LEAD", "Lestada", "made-at-home", "DjangoDiabolik", "iceberrie", "Eiganerd", "quirky-fair", "Litterboxd", "LIÉGE-BASTOGNE-LIÉGE", "Artvanz", "PARIS-ROUBAIX", "Paterburg", "Kruisberg", "Juízo", "TfLOfficial", "biomagnification-the", "site-central", "test-were", "readers-missed", "Urmansky", "Wonkporn", "Kiraracha", "Borgeglio", "continenets", "Melo-who", "aaron-bassler", "jere-melo", "jere-melos-medical-exam", "opium-in-mendocino", "Pro-Riders", "Ribeezie", "-0807830123", "-0268018771", "degree-spin", "-0300164299", "mssinglemama", "-0802844200", "Kgw", "digitally-connected", "disengaged-drained", "everyday-folks-turned-trend-watchers", "Gegeric", "Discouunt", "TheManRepeller.com", "Stylebykling.nowmanifest.com", "Veltrop", "Meme-strength", "Android-in", "Catholic-sponsored", "Experimentused", "csstester", "safety-none", "Yu-Hsi", "Hard-Landing", "Hangzho", "family-defense", "snakewood", "too-little-soap", "too-little-water", "micro-dam", "Jaronu", "Manichi", "US-Mongolia", "105888692826528", "Cacocun", "Ramriezes", ];
}