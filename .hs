{-# OPTIONS_GHC -w #-}
-- SlowQL SQL Parser
module SlowQL.SQL.Parser where
import Data.Char
import qualified SlowQL.Record.DataType as DT
import qualified Data.ByteString.Char8 as BC
import Data.Either
import Data.Maybe
import qualified Data.Array as Happy_Data_Array
import qualified Data.Bits as Bits
import Control.Applicative(Applicative(..))
import Control.Monad (ap)

-- parser produced by Happy Version 1.19.9

data HappyAbsSyn t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19 t20 t21 t22 t23 t24 t25 t26 t27 t28 t29 t30 t31 t32 t33 t34 t35 t36 t37
	= HappyTerminal (Token)
	| HappyErrorToken Int
	| HappyAbsSyn4 t4
	| HappyAbsSyn5 t5
	| HappyAbsSyn6 t6
	| HappyAbsSyn7 t7
	| HappyAbsSyn8 t8
	| HappyAbsSyn9 t9
	| HappyAbsSyn10 t10
	| HappyAbsSyn11 t11
	| HappyAbsSyn12 t12
	| HappyAbsSyn13 t13
	| HappyAbsSyn14 t14
	| HappyAbsSyn15 t15
	| HappyAbsSyn16 t16
	| HappyAbsSyn17 t17
	| HappyAbsSyn18 t18
	| HappyAbsSyn19 t19
	| HappyAbsSyn20 t20
	| HappyAbsSyn21 t21
	| HappyAbsSyn22 t22
	| HappyAbsSyn23 t23
	| HappyAbsSyn24 t24
	| HappyAbsSyn25 t25
	| HappyAbsSyn26 t26
	| HappyAbsSyn27 t27
	| HappyAbsSyn28 t28
	| HappyAbsSyn29 t29
	| HappyAbsSyn30 t30
	| HappyAbsSyn31 t31
	| HappyAbsSyn32 t32
	| HappyAbsSyn33 t33
	| HappyAbsSyn34 t34
	| HappyAbsSyn35 t35
	| HappyAbsSyn36 t36
	| HappyAbsSyn37 t37

happyExpList :: Happy_Data_Array.Array Int Int
happyExpList = Happy_Data_Array.listArray (0,238) ([0,0,2016,288,128,0,0,32256,4608,2048,0,0,0,32,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7168,0,0,0,0,49152,1,0,0,0,32768,0,0,0,0,32768,0,0,0,0,0,0,2048,0,0,0,2048,0,0,0,0,0,4,0,0,0,0,8256,64,0,0,0,128,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,32768,0,0,0,0,0,0,512,0,0,0,128,0,0,0,0,2048,0,0,0,0,16384,0,0,0,0,32768,0,0,0,0,0,0,0,1024,0,0,2048,0,0,0,0,32768,0,0,0,0,0,8,0,0,0,0,128,0,0,0,0,2048,0,0,0,0,32768,0,0,0,0,0,0,0,0,0,0,128,0,0,0,0,0,4,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,32768,0,0,0,0,0,8,0,0,0,0,128,0,0,0,0,256,0,0,0,0,0,0,16,0,0,0,1024,0,0,0,0,0,0,0,0,0,2048,0,0,0,0,0,64,0,0,0,0,8,0,0,0,0,0,0,0,0,0,256,0,0,0,0,16384,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1024,0,0,0,0,32768,16384,1024,0,0,0,0,0,0,0,0,32768,0,0,0,0,1024,0,0,0,0,0,15360,0,0,0,0,0,8,0,0,0,0,128,0,0,0,2048,0,0,0,0,32768,0,0,0,0,0,8,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,61504,3,0,0,48,32,0,0,0,32768,0,0,0,0,1024,0,0,0,0,0,0,4096,0,0,0,48,32,0,0,0,128,0,0,0,0,256,0,0,0,0,0,128,0,0,0,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,56,32,0,0,0,0,768,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1024,0,0,0,0,128,0,0,0,0,0,4,0,0,0,0,64,0,0,0,0,0,16,0,0,0,0,256,0,0,0,2048,1024,64,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8192,0,0,0,0,0,0,0,0,0,0,16,0,0,0,512,0,0,0,0,0,4096,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,48,32,0,0,0,0,0,0,0,0,2048,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2048,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,128,0,0,32768,0,0,0,0,0,1024,0,0,0,0,128,0,0,0,0,0,8,0,0,0,0,0,0,0,0
	])

{-# NOINLINE happyExpListPerState #-}
happyExpListPerState st =
    token_strs_expected
  where token_strs = ["error","%dummy","%start_parseTokens","SQL","SQLStatement","CreateDatabase","DropDatabase","CreateTable","DomainList","DomainDesc","MaxLength","Nullable","DropTable","UseDatabase","ShowDatabases","ShowDatabase","ShowTables","DescribeTable","InsertStmt","ValueList","Value","TableList","IsNull","WhereOp","WhereClause","Column","Expr","Op","ColumnList","IdtList","SelectStmt","UpdateStmt","DeleteStmt","SetClause","Assignment","CreateIndex","DropIndex","create","drop","select","delete","insert","update","from","where","';'","','","IDENTIFIER","STRING","INT","database","table","index","use","'('","')'","show","databases","int","char","float","date","primary","key","not","null","tables","into","values","set","is","desc","asc","and","foreign","references","'='","'<>'","'<='","'>='","'<'","'>'","'.'","%eof"]
        bit_start = st * 84
        bit_end = (st + 1) * 84
        read_bit = readArrayBit happyExpList
        bits = map read_bit [bit_start..bit_end - 1]
        bits_indexed = zip bits [0..83]
        token_strs_expected = concatMap f bits_indexed
        f (False, _) = []
        f (True, nr) = [token_strs !! nr]

action_0 (38) = happyShift action_18
action_0 (39) = happyShift action_19
action_0 (40) = happyShift action_20
action_0 (41) = happyShift action_21
action_0 (42) = happyShift action_22
action_0 (43) = happyShift action_23
action_0 (54) = happyShift action_24
action_0 (57) = happyShift action_25
action_0 (72) = happyShift action_26
action_0 (4) = happyGoto action_27
action_0 (5) = happyGoto action_2
action_0 (6) = happyGoto action_3
action_0 (7) = happyGoto action_4
action_0 (8) = happyGoto action_5
action_0 (13) = happyGoto action_6
action_0 (14) = happyGoto action_7
action_0 (15) = happyGoto action_8
action_0 (16) = happyGoto action_9
action_0 (17) = happyGoto action_10
action_0 (18) = happyGoto action_11
action_0 (19) = happyGoto action_12
action_0 (31) = happyGoto action_13
action_0 (32) = happyGoto action_14
action_0 (33) = happyGoto action_15
action_0 (36) = happyGoto action_16
action_0 (37) = happyGoto action_17
action_0 _ = happyFail (happyExpListPerState 0)

action_1 (38) = happyShift action_18
action_1 (39) = happyShift action_19
action_1 (40) = happyShift action_20
action_1 (41) = happyShift action_21
action_1 (42) = happyShift action_22
action_1 (43) = happyShift action_23
action_1 (54) = happyShift action_24
action_1 (57) = happyShift action_25
action_1 (72) = happyShift action_26
action_1 (5) = happyGoto action_2
action_1 (6) = happyGoto action_3
action_1 (7) = happyGoto action_4
action_1 (8) = happyGoto action_5
action_1 (13) = happyGoto action_6
action_1 (14) = happyGoto action_7
action_1 (15) = happyGoto action_8
action_1 (16) = happyGoto action_9
action_1 (17) = happyGoto action_10
action_1 (18) = happyGoto action_11
action_1 (19) = happyGoto action_12
action_1 (31) = happyGoto action_13
action_1 (32) = happyGoto action_14
action_1 (33) = happyGoto action_15
action_1 (36) = happyGoto action_16
action_1 (37) = happyGoto action_17
action_1 _ = happyFail (happyExpListPerState 1)

action_2 (46) = happyShift action_45
action_2 _ = happyFail (happyExpListPerState 2)

action_3 _ = happyReduce_2

action_4 _ = happyReduce_3

action_5 _ = happyReduce_4

action_6 _ = happyReduce_5

action_7 _ = happyReduce_6

action_8 _ = happyReduce_7

action_9 _ = happyReduce_8

action_10 _ = happyReduce_15

action_11 _ = happyReduce_16

action_12 _ = happyReduce_11

action_13 _ = happyReduce_9

action_14 _ = happyReduce_10

action_15 _ = happyReduce_12

action_16 _ = happyReduce_13

action_17 _ = happyReduce_14

action_18 (51) = happyShift action_42
action_18 (52) = happyShift action_43
action_18 (53) = happyShift action_44
action_18 _ = happyFail (happyExpListPerState 18)

action_19 (51) = happyShift action_39
action_19 (52) = happyShift action_40
action_19 (53) = happyShift action_41
action_19 _ = happyFail (happyExpListPerState 19)

action_20 (48) = happyShift action_38
action_20 (26) = happyGoto action_36
action_20 (29) = happyGoto action_37
action_20 _ = happyFail (happyExpListPerState 20)

action_21 (44) = happyShift action_35
action_21 _ = happyFail (happyExpListPerState 21)

action_22 (68) = happyShift action_34
action_22 _ = happyFail (happyExpListPerState 22)

action_23 (48) = happyShift action_33
action_23 _ = happyFail (happyExpListPerState 23)

action_24 (51) = happyShift action_32
action_24 _ = happyFail (happyExpListPerState 24)

action_25 (51) = happyShift action_29
action_25 (58) = happyShift action_30
action_25 (67) = happyShift action_31
action_25 _ = happyFail (happyExpListPerState 25)

action_26 (48) = happyShift action_28
action_26 _ = happyFail (happyExpListPerState 26)

action_27 (84) = happyAccept
action_27 _ = happyFail (happyExpListPerState 27)

action_28 _ = happyReduce_37

action_29 _ = happyReduce_35

action_30 _ = happyReduce_34

action_31 _ = happyReduce_36

action_32 (48) = happyShift action_58
action_32 _ = happyFail (happyExpListPerState 32)

action_33 (70) = happyShift action_57
action_33 _ = happyFail (happyExpListPerState 33)

action_34 (48) = happyShift action_56
action_34 _ = happyFail (happyExpListPerState 34)

action_35 (48) = happyShift action_55
action_35 _ = happyFail (happyExpListPerState 35)

action_36 (47) = happyShift action_54
action_36 _ = happyReduce_62

action_37 (44) = happyShift action_53
action_37 _ = happyFail (happyExpListPerState 37)

action_38 (83) = happyShift action_52
action_38 _ = happyReduce_52

action_39 (48) = happyShift action_51
action_39 _ = happyFail (happyExpListPerState 39)

action_40 (48) = happyShift action_50
action_40 _ = happyFail (happyExpListPerState 40)

action_41 (48) = happyShift action_49
action_41 _ = happyFail (happyExpListPerState 41)

action_42 (48) = happyShift action_48
action_42 _ = happyFail (happyExpListPerState 42)

action_43 (48) = happyShift action_47
action_43 _ = happyFail (happyExpListPerState 43)

action_44 (48) = happyShift action_46
action_44 _ = happyFail (happyExpListPerState 44)

action_45 _ = happyReduce_1

action_46 (48) = happyShift action_67
action_46 (30) = happyGoto action_69
action_46 _ = happyFail (happyExpListPerState 46)

action_47 (55) = happyShift action_68
action_47 _ = happyFail (happyExpListPerState 47)

action_48 _ = happyReduce_17

action_49 (48) = happyShift action_67
action_49 (30) = happyGoto action_66
action_49 _ = happyFail (happyExpListPerState 49)

action_50 _ = happyReduce_32

action_51 _ = happyReduce_18

action_52 (48) = happyShift action_65
action_52 _ = happyFail (happyExpListPerState 52)

action_53 (48) = happyShift action_64
action_53 (22) = happyGoto action_63
action_53 _ = happyFail (happyExpListPerState 53)

action_54 (48) = happyShift action_38
action_54 (26) = happyGoto action_36
action_54 (29) = happyGoto action_62
action_54 _ = happyFail (happyExpListPerState 54)

action_55 (45) = happyShift action_61
action_55 _ = happyFail (happyExpListPerState 55)

action_56 (69) = happyShift action_60
action_56 _ = happyFail (happyExpListPerState 56)

action_57 (55) = happyShift action_59
action_57 _ = happyFail (happyExpListPerState 57)

action_58 _ = happyReduce_33

action_59 (48) = happyShift action_84
action_59 (34) = happyGoto action_82
action_59 (35) = happyGoto action_83
action_59 _ = happyFail (happyExpListPerState 59)

action_60 (55) = happyShift action_81
action_60 _ = happyFail (happyExpListPerState 60)

action_61 (48) = happyShift action_38
action_61 (24) = happyGoto action_78
action_61 (25) = happyGoto action_79
action_61 (26) = happyGoto action_80
action_61 _ = happyFail (happyExpListPerState 61)

action_62 _ = happyReduce_63

action_63 (45) = happyShift action_77
action_63 _ = happyFail (happyExpListPerState 63)

action_64 (47) = happyShift action_76
action_64 _ = happyReduce_44

action_65 _ = happyReduce_53

action_66 _ = happyReduce_73

action_67 (47) = happyShift action_75
action_67 _ = happyReduce_64

action_68 (48) = happyShift action_72
action_68 (63) = happyShift action_73
action_68 (75) = happyShift action_74
action_68 (9) = happyGoto action_70
action_68 (10) = happyGoto action_71
action_68 _ = happyFail (happyExpListPerState 68)

action_69 _ = happyReduce_72

action_70 (56) = happyShift action_112
action_70 _ = happyFail (happyExpListPerState 70)

action_71 (47) = happyShift action_111
action_71 _ = happyReduce_20

action_72 (59) = happyShift action_107
action_72 (60) = happyShift action_108
action_72 (61) = happyShift action_109
action_72 (62) = happyShift action_110
action_72 _ = happyFail (happyExpListPerState 72)

action_73 (64) = happyShift action_106
action_73 _ = happyFail (happyExpListPerState 73)

action_74 (64) = happyShift action_105
action_74 _ = happyFail (happyExpListPerState 74)

action_75 (48) = happyShift action_67
action_75 (30) = happyGoto action_104
action_75 _ = happyFail (happyExpListPerState 75)

action_76 (48) = happyShift action_64
action_76 (22) = happyGoto action_103
action_76 _ = happyFail (happyExpListPerState 76)

action_77 (48) = happyShift action_38
action_77 (24) = happyGoto action_78
action_77 (25) = happyGoto action_102
action_77 (26) = happyGoto action_80
action_77 _ = happyFail (happyExpListPerState 77)

action_78 (74) = happyShift action_101
action_78 _ = happyReduce_50

action_79 _ = happyReduce_68

action_80 (71) = happyShift action_94
action_80 (77) = happyShift action_95
action_80 (78) = happyShift action_96
action_80 (79) = happyShift action_97
action_80 (80) = happyShift action_98
action_80 (81) = happyShift action_99
action_80 (82) = happyShift action_100
action_80 (28) = happyGoto action_93
action_80 _ = happyFail (happyExpListPerState 80)

action_81 (49) = happyShift action_90
action_81 (50) = happyShift action_91
action_81 (66) = happyShift action_92
action_81 (20) = happyGoto action_88
action_81 (21) = happyGoto action_89
action_81 _ = happyFail (happyExpListPerState 81)

action_82 (56) = happyShift action_87
action_82 _ = happyFail (happyExpListPerState 82)

action_83 (47) = happyShift action_86
action_83 _ = happyReduce_69

action_84 (77) = happyShift action_85
action_84 _ = happyFail (happyExpListPerState 84)

action_85 (49) = happyShift action_90
action_85 (50) = happyShift action_91
action_85 (66) = happyShift action_92
action_85 (21) = happyGoto action_133
action_85 _ = happyFail (happyExpListPerState 85)

action_86 (48) = happyShift action_84
action_86 (34) = happyGoto action_132
action_86 (35) = happyGoto action_83
action_86 _ = happyFail (happyExpListPerState 86)

action_87 (45) = happyShift action_131
action_87 _ = happyFail (happyExpListPerState 87)

action_88 (56) = happyShift action_130
action_88 _ = happyFail (happyExpListPerState 88)

action_89 (47) = happyShift action_129
action_89 _ = happyReduce_39

action_90 _ = happyReduce_41

action_91 _ = happyReduce_42

action_92 _ = happyReduce_43

action_93 (48) = happyShift action_38
action_93 (49) = happyShift action_90
action_93 (50) = happyShift action_91
action_93 (66) = happyShift action_92
action_93 (21) = happyGoto action_126
action_93 (26) = happyGoto action_127
action_93 (27) = happyGoto action_128
action_93 _ = happyFail (happyExpListPerState 93)

action_94 (65) = happyShift action_124
action_94 (66) = happyShift action_125
action_94 (23) = happyGoto action_123
action_94 _ = happyFail (happyExpListPerState 94)

action_95 _ = happyReduce_56

action_96 _ = happyReduce_57

action_97 _ = happyReduce_58

action_98 _ = happyReduce_59

action_99 _ = happyReduce_60

action_100 _ = happyReduce_61

action_101 (48) = happyShift action_38
action_101 (24) = happyGoto action_78
action_101 (25) = happyGoto action_122
action_101 (26) = happyGoto action_80
action_101 _ = happyFail (happyExpListPerState 101)

action_102 _ = happyReduce_66

action_103 _ = happyReduce_45

action_104 _ = happyReduce_65

action_105 (55) = happyShift action_121
action_105 _ = happyFail (happyExpListPerState 105)

action_106 (48) = happyShift action_120
action_106 _ = happyFail (happyExpListPerState 106)

action_107 (55) = happyShift action_118
action_107 (11) = happyGoto action_119
action_107 _ = happyReduce_28

action_108 (55) = happyShift action_118
action_108 (11) = happyGoto action_117
action_108 _ = happyReduce_28

action_109 (65) = happyShift action_115
action_109 (12) = happyGoto action_116
action_109 _ = happyReduce_30

action_110 (65) = happyShift action_115
action_110 (12) = happyGoto action_114
action_110 _ = happyReduce_30

action_111 (48) = happyShift action_72
action_111 (63) = happyShift action_73
action_111 (75) = happyShift action_74
action_111 (9) = happyGoto action_113
action_111 (10) = happyGoto action_71
action_111 _ = happyFail (happyExpListPerState 111)

action_112 _ = happyReduce_19

action_113 _ = happyReduce_21

action_114 _ = happyReduce_25

action_115 (66) = happyShift action_141
action_115 _ = happyFail (happyExpListPerState 115)

action_116 _ = happyReduce_24

action_117 (65) = happyShift action_115
action_117 (12) = happyGoto action_140
action_117 _ = happyReduce_30

action_118 (50) = happyShift action_139
action_118 _ = happyFail (happyExpListPerState 118)

action_119 (65) = happyShift action_115
action_119 (12) = happyGoto action_138
action_119 _ = happyReduce_30

action_120 _ = happyReduce_26

action_121 (48) = happyShift action_137
action_121 _ = happyFail (happyExpListPerState 121)

action_122 _ = happyReduce_51

action_123 _ = happyReduce_49

action_124 (66) = happyShift action_136
action_124 _ = happyFail (happyExpListPerState 124)

action_125 _ = happyReduce_46

action_126 _ = happyReduce_55

action_127 _ = happyReduce_54

action_128 _ = happyReduce_48

action_129 (49) = happyShift action_90
action_129 (50) = happyShift action_91
action_129 (66) = happyShift action_92
action_129 (20) = happyGoto action_135
action_129 (21) = happyGoto action_89
action_129 _ = happyFail (happyExpListPerState 129)

action_130 _ = happyReduce_38

action_131 (48) = happyShift action_38
action_131 (24) = happyGoto action_78
action_131 (25) = happyGoto action_134
action_131 (26) = happyGoto action_80
action_131 _ = happyFail (happyExpListPerState 131)

action_132 _ = happyReduce_70

action_133 _ = happyReduce_71

action_134 _ = happyReduce_67

action_135 _ = happyReduce_40

action_136 _ = happyReduce_47

action_137 (56) = happyShift action_143
action_137 _ = happyFail (happyExpListPerState 137)

action_138 _ = happyReduce_23

action_139 (56) = happyShift action_142
action_139 _ = happyFail (happyExpListPerState 139)

action_140 _ = happyReduce_22

action_141 _ = happyReduce_31

action_142 _ = happyReduce_29

action_143 (76) = happyShift action_144
action_143 _ = happyFail (happyExpListPerState 143)

action_144 (48) = happyShift action_145
action_144 _ = happyFail (happyExpListPerState 144)

action_145 (55) = happyShift action_146
action_145 _ = happyFail (happyExpListPerState 145)

action_146 (48) = happyShift action_147
action_146 _ = happyFail (happyExpListPerState 146)

action_147 (56) = happyShift action_148
action_147 _ = happyFail (happyExpListPerState 147)

action_148 _ = happyReduce_27

happyReduce_1 = happySpecReduce_2  4 happyReduction_1
happyReduction_1 _
	(HappyAbsSyn5  happy_var_1)
	 =  HappyAbsSyn4
		 (happy_var_1
	)
happyReduction_1 _ _  = notHappyAtAll 

happyReduce_2 = happySpecReduce_1  5 happyReduction_2
happyReduction_2 (HappyAbsSyn6  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_2 _  = notHappyAtAll 

happyReduce_3 = happySpecReduce_1  5 happyReduction_3
happyReduction_3 (HappyAbsSyn7  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_3 _  = notHappyAtAll 

happyReduce_4 = happySpecReduce_1  5 happyReduction_4
happyReduction_4 (HappyAbsSyn8  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_4 _  = notHappyAtAll 

happyReduce_5 = happySpecReduce_1  5 happyReduction_5
happyReduction_5 (HappyAbsSyn13  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_5 _  = notHappyAtAll 

happyReduce_6 = happySpecReduce_1  5 happyReduction_6
happyReduction_6 (HappyAbsSyn14  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_6 _  = notHappyAtAll 

happyReduce_7 = happySpecReduce_1  5 happyReduction_7
happyReduction_7 (HappyAbsSyn15  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_7 _  = notHappyAtAll 

happyReduce_8 = happySpecReduce_1  5 happyReduction_8
happyReduction_8 (HappyAbsSyn16  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_8 _  = notHappyAtAll 

happyReduce_9 = happySpecReduce_1  5 happyReduction_9
happyReduction_9 (HappyAbsSyn31  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_9 _  = notHappyAtAll 

happyReduce_10 = happySpecReduce_1  5 happyReduction_10
happyReduction_10 (HappyAbsSyn32  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_10 _  = notHappyAtAll 

happyReduce_11 = happySpecReduce_1  5 happyReduction_11
happyReduction_11 (HappyAbsSyn19  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_11 _  = notHappyAtAll 

happyReduce_12 = happySpecReduce_1  5 happyReduction_12
happyReduction_12 (HappyAbsSyn33  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_12 _  = notHappyAtAll 

happyReduce_13 = happySpecReduce_1  5 happyReduction_13
happyReduction_13 (HappyAbsSyn36  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_13 _  = notHappyAtAll 

happyReduce_14 = happySpecReduce_1  5 happyReduction_14
happyReduction_14 (HappyAbsSyn37  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_14 _  = notHappyAtAll 

happyReduce_15 = happySpecReduce_1  5 happyReduction_15
happyReduction_15 (HappyAbsSyn17  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_15 _  = notHappyAtAll 

happyReduce_16 = happySpecReduce_1  5 happyReduction_16
happyReduction_16 (HappyAbsSyn18  happy_var_1)
	 =  HappyAbsSyn5
		 (happy_var_1
	)
happyReduction_16 _  = notHappyAtAll 

happyReduce_17 = happySpecReduce_3  6 happyReduction_17
happyReduction_17 (HappyTerminal (TokenIdentifier happy_var_3))
	_
	_
	 =  HappyAbsSyn6
		 (CreateDatabase happy_var_3
	)
happyReduction_17 _ _ _  = notHappyAtAll 

happyReduce_18 = happySpecReduce_3  7 happyReduction_18
happyReduction_18 (HappyTerminal (TokenIdentifier happy_var_3))
	_
	_
	 =  HappyAbsSyn7
		 (DropDatabase happy_var_3
	)
happyReduction_18 _ _ _  = notHappyAtAll 

happyReduce_19 = happyReduce 6 8 happyReduction_19
happyReduction_19 (_ `HappyStk`
	(HappyAbsSyn9  happy_var_5) `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_3)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn8
		 (CreateTable happy_var_3 happy_var_5
	) `HappyStk` happyRest

happyReduce_20 = happySpecReduce_1  9 happyReduction_20
happyReduction_20 (HappyAbsSyn10  happy_var_1)
	 =  HappyAbsSyn9
		 (happy_var_1 (TableDescription [] Nothing)
	)
happyReduction_20 _  = notHappyAtAll 

happyReduce_21 = happySpecReduce_3  9 happyReduction_21
happyReduction_21 (HappyAbsSyn9  happy_var_3)
	_
	(HappyAbsSyn10  happy_var_1)
	 =  HappyAbsSyn9
		 (happy_var_1 happy_var_3
	)
happyReduction_21 _ _ _  = notHappyAtAll 

happyReduce_22 = happyReduce 4 10 happyReduction_22
happyReduction_22 ((HappyAbsSyn12  happy_var_4) `HappyStk`
	(HappyAbsSyn11  happy_var_3) `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_1)) `HappyStk`
	happyRest)
	 = HappyAbsSyn10
		 (appendParam $ DT.TVarCharParam (DT.TGeneralParam (BC.pack happy_var_1) happy_var_4) happy_var_3
	) `HappyStk` happyRest

happyReduce_23 = happyReduce 4 10 happyReduction_23
happyReduction_23 ((HappyAbsSyn12  happy_var_4) `HappyStk`
	(HappyAbsSyn11  happy_var_3) `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_1)) `HappyStk`
	happyRest)
	 = HappyAbsSyn10
		 (appendParam $ DT.TIntParam (DT.TGeneralParam (BC.pack happy_var_1) happy_var_4) happy_var_3
	) `HappyStk` happyRest

happyReduce_24 = happySpecReduce_3  10 happyReduction_24
happyReduction_24 (HappyAbsSyn12  happy_var_3)
	_
	(HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn10
		 (appendParam $ DT.TFloatParam (DT.TGeneralParam (BC.pack happy_var_1) happy_var_3)
	)
happyReduction_24 _ _ _  = notHappyAtAll 

happyReduce_25 = happySpecReduce_3  10 happyReduction_25
happyReduction_25 (HappyAbsSyn12  happy_var_3)
	_
	(HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn10
		 (appendParam $ DT.TFloatParam (DT.TGeneralParam (BC.pack happy_var_1) happy_var_3)
	)
happyReduction_25 _ _ _  = notHappyAtAll 

happyReduce_26 = happySpecReduce_3  10 happyReduction_26
happyReduction_26 (HappyTerminal (TokenIdentifier happy_var_3))
	_
	_
	 =  HappyAbsSyn10
		 (setPrimary happy_var_3
	)
happyReduction_26 _ _ _  = notHappyAtAll 

happyReduce_27 = happyReduce 10 10 happyReduction_27
happyReduction_27 (_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_9)) `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_7)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_4)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn10
		 (appendForeign happy_var_4 happy_var_7 happy_var_9
	) `HappyStk` happyRest

happyReduce_28 = happySpecReduce_0  11 happyReduction_28
happyReduction_28  =  HappyAbsSyn11
		 (255
	)

happyReduce_29 = happySpecReduce_3  11 happyReduction_29
happyReduction_29 _
	(HappyTerminal (TokenInteger happy_var_2))
	_
	 =  HappyAbsSyn11
		 (happy_var_2
	)
happyReduction_29 _ _ _  = notHappyAtAll 

happyReduce_30 = happySpecReduce_0  12 happyReduction_30
happyReduction_30  =  HappyAbsSyn12
		 (True
	)

happyReduce_31 = happySpecReduce_2  12 happyReduction_31
happyReduction_31 _
	_
	 =  HappyAbsSyn12
		 (False
	)

happyReduce_32 = happySpecReduce_3  13 happyReduction_32
happyReduction_32 (HappyTerminal (TokenIdentifier happy_var_3))
	_
	_
	 =  HappyAbsSyn13
		 (DropTable happy_var_3
	)
happyReduction_32 _ _ _  = notHappyAtAll 

happyReduce_33 = happySpecReduce_3  14 happyReduction_33
happyReduction_33 (HappyTerminal (TokenIdentifier happy_var_3))
	_
	_
	 =  HappyAbsSyn14
		 (UseDatabase happy_var_3
	)
happyReduction_33 _ _ _  = notHappyAtAll 

happyReduce_34 = happySpecReduce_2  15 happyReduction_34
happyReduction_34 _
	_
	 =  HappyAbsSyn15
		 (ShowDatabases
	)

happyReduce_35 = happySpecReduce_2  16 happyReduction_35
happyReduction_35 _
	_
	 =  HappyAbsSyn16
		 (ShowDatabase
	)

happyReduce_36 = happySpecReduce_2  17 happyReduction_36
happyReduction_36 _
	_
	 =  HappyAbsSyn17
		 (ShowTables
	)

happyReduce_37 = happySpecReduce_2  18 happyReduction_37
happyReduction_37 (HappyTerminal (TokenIdentifier happy_var_2))
	_
	 =  HappyAbsSyn18
		 (DescribeTable happy_var_2
	)
happyReduction_37 _ _  = notHappyAtAll 

happyReduce_38 = happyReduce 7 19 happyReduction_38
happyReduction_38 (_ `HappyStk`
	(HappyAbsSyn20  happy_var_6) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_3)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn19
		 (InsertStmt happy_var_3 happy_var_6
	) `HappyStk` happyRest

happyReduce_39 = happySpecReduce_1  20 happyReduction_39
happyReduction_39 (HappyAbsSyn21  happy_var_1)
	 =  HappyAbsSyn20
		 ([happy_var_1]
	)
happyReduction_39 _  = notHappyAtAll 

happyReduce_40 = happySpecReduce_3  20 happyReduction_40
happyReduction_40 (HappyAbsSyn20  happy_var_3)
	_
	(HappyAbsSyn21  happy_var_1)
	 =  HappyAbsSyn20
		 ([happy_var_1:happy_var_3]
	)
happyReduction_40 _ _ _  = notHappyAtAll 

happyReduce_41 = happySpecReduce_1  21 happyReduction_41
happyReduction_41 (HappyTerminal (TokenString happy_var_1))
	 =  HappyAbsSyn21
		 (DT.ValChar (Just happy_var_1)
	)
happyReduction_41 _  = notHappyAtAll 

happyReduce_42 = happySpecReduce_1  21 happyReduction_42
happyReduction_42 (HappyTerminal (TokenInteger happy_var_1))
	 =  HappyAbsSyn21
		 (DT.ValInt (Just happy_var_1)
	)
happyReduction_42 _  = notHappyAtAll 

happyReduce_43 = happySpecReduce_1  21 happyReduction_43
happyReduction_43 _
	 =  HappyAbsSyn21
		 (DT.ValNull
	)

happyReduce_44 = happySpecReduce_1  22 happyReduction_44
happyReduction_44 (HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn22
		 ([happy_var_1]
	)
happyReduction_44 _  = notHappyAtAll 

happyReduce_45 = happySpecReduce_3  22 happyReduction_45
happyReduction_45 (HappyAbsSyn22  happy_var_3)
	_
	(HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn22
		 (happy_var_1:happy_var_3
	)
happyReduction_45 _ _ _  = notHappyAtAll 

happyReduce_46 = happySpecReduce_1  23 happyReduction_46
happyReduction_46 _
	 =  HappyAbsSyn23
		 (True
	)

happyReduce_47 = happySpecReduce_2  23 happyReduction_47
happyReduction_47 _
	_
	 =  HappyAbsSyn23
		 (False
	)

happyReduce_48 = happySpecReduce_3  24 happyReduction_48
happyReduction_48 (HappyAbsSyn27  happy_var_3)
	(HappyAbsSyn28  happy_var_2)
	(HappyAbsSyn26  happy_var_1)
	 =  HappyAbsSyn24
		 (WhereOp happy_var_1 happy_var_2 happy_var_3
	)
happyReduction_48 _ _ _  = notHappyAtAll 

happyReduce_49 = happySpecReduce_3  24 happyReduction_49
happyReduction_49 (HappyAbsSyn23  happy_var_3)
	_
	_
	 =  HappyAbsSyn24
		 (WhereIsNull happy_var_3
	)
happyReduction_49 _ _ _  = notHappyAtAll 

happyReduce_50 = happySpecReduce_1  25 happyReduction_50
happyReduction_50 (HappyAbsSyn24  happy_var_1)
	 =  HappyAbsSyn25
		 (happy_var_1
	)
happyReduction_50 _  = notHappyAtAll 

happyReduce_51 = happySpecReduce_3  25 happyReduction_51
happyReduction_51 (HappyAbsSyn25  happy_var_3)
	_
	(HappyAbsSyn24  happy_var_1)
	 =  HappyAbsSyn25
		 (WhereAnd happy_var_1 happy_var_3
	)
happyReduction_51 _ _ _  = notHappyAtAll 

happyReduce_52 = happySpecReduce_1  26 happyReduction_52
happyReduction_52 (HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn26
		 (LocalCol happy_var_1
	)
happyReduction_52 _  = notHappyAtAll 

happyReduce_53 = happySpecReduce_3  26 happyReduction_53
happyReduction_53 (HappyTerminal (TokenIdentifier happy_var_3))
	_
	(HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn26
		 (ForeignCol happy_var_1 happy_var_3
	)
happyReduction_53 _ _ _  = notHappyAtAll 

happyReduce_54 = happySpecReduce_1  27 happyReduction_54
happyReduction_54 (HappyAbsSyn26  happy_var_1)
	 =  HappyAbsSyn27
		 (ExprC happy_var_1
	)
happyReduction_54 _  = notHappyAtAll 

happyReduce_55 = happySpecReduce_1  27 happyReduction_55
happyReduction_55 (HappyAbsSyn21  happy_var_1)
	 =  HappyAbsSyn27
		 (ExprV happy_var_1
	)
happyReduction_55 _  = notHappyAtAll 

happyReduce_56 = happySpecReduce_1  28 happyReduction_56
happyReduction_56 _
	 =  HappyAbsSyn28
		 (OpEq
	)

happyReduce_57 = happySpecReduce_1  28 happyReduction_57
happyReduction_57 _
	 =  HappyAbsSyn28
		 (OpNeq
	)

happyReduce_58 = happySpecReduce_1  28 happyReduction_58
happyReduction_58 _
	 =  HappyAbsSyn28
		 (OpLeq
	)

happyReduce_59 = happySpecReduce_1  28 happyReduction_59
happyReduction_59 _
	 =  HappyAbsSyn28
		 (OpGeq
	)

happyReduce_60 = happySpecReduce_1  28 happyReduction_60
happyReduction_60 _
	 =  HappyAbsSyn28
		 (OpLt
	)

happyReduce_61 = happySpecReduce_1  28 happyReduction_61
happyReduction_61 _
	 =  HappyAbsSyn28
		 (OpGt
	)

happyReduce_62 = happySpecReduce_1  29 happyReduction_62
happyReduction_62 (HappyAbsSyn26  happy_var_1)
	 =  HappyAbsSyn29
		 ([happy_var_1]
	)
happyReduction_62 _  = notHappyAtAll 

happyReduce_63 = happySpecReduce_3  29 happyReduction_63
happyReduction_63 (HappyAbsSyn29  happy_var_3)
	_
	(HappyAbsSyn26  happy_var_1)
	 =  HappyAbsSyn29
		 (happy_var_1:happy_var_3
	)
happyReduction_63 _ _ _  = notHappyAtAll 

happyReduce_64 = happySpecReduce_1  30 happyReduction_64
happyReduction_64 (HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn30
		 ([happy_var_1]
	)
happyReduction_64 _  = notHappyAtAll 

happyReduce_65 = happySpecReduce_3  30 happyReduction_65
happyReduction_65 (HappyAbsSyn30  happy_var_3)
	_
	(HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn30
		 (happy_var_1:happy_var_3
	)
happyReduction_65 _ _ _  = notHappyAtAll 

happyReduce_66 = happyReduce 6 31 happyReduction_66
happyReduction_66 (_ `HappyStk`
	(HappyTerminal happy_var_5) `HappyStk`
	(HappyAbsSyn22  happy_var_4) `HappyStk`
	_ `HappyStk`
	(HappyAbsSyn29  happy_var_2) `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn31
		 (SelectStmt happy_var_2 happy_var_4 happy_var_5
	) `HappyStk` happyRest

happyReduce_67 = happyReduce 8 32 happyReduction_67
happyReduction_67 ((HappyAbsSyn25  happy_var_8) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	(HappyAbsSyn34  happy_var_5) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_2)) `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn32
		 (UpdateStmt happy_var_2 happy_var_5 happy_var_8
	) `HappyStk` happyRest

happyReduce_68 = happyReduce 5 33 happyReduction_68
happyReduction_68 ((HappyAbsSyn25  happy_var_5) `HappyStk`
	_ `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_3)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn33
		 (DeleteStmt happy_var_3 happy_var_5
	) `HappyStk` happyRest

happyReduce_69 = happySpecReduce_1  34 happyReduction_69
happyReduction_69 (HappyAbsSyn35  happy_var_1)
	 =  HappyAbsSyn34
		 ([happy_var_1]
	)
happyReduction_69 _  = notHappyAtAll 

happyReduce_70 = happySpecReduce_3  34 happyReduction_70
happyReduction_70 (HappyAbsSyn34  happy_var_3)
	_
	(HappyAbsSyn35  happy_var_1)
	 =  HappyAbsSyn34
		 (happy_var_1:happy_var_3
	)
happyReduction_70 _ _ _  = notHappyAtAll 

happyReduce_71 = happySpecReduce_3  35 happyReduction_71
happyReduction_71 (HappyAbsSyn21  happy_var_3)
	_
	(HappyTerminal (TokenIdentifier happy_var_1))
	 =  HappyAbsSyn35
		 ((happy_var_1, happy_var_3)
	)
happyReduction_71 _ _ _  = notHappyAtAll 

happyReduce_72 = happyReduce 4 36 happyReduction_72
happyReduction_72 ((HappyAbsSyn30  happy_var_4) `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_3)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn36
		 (CreateIndex happy_var_3 happy_var_4
	) `HappyStk` happyRest

happyReduce_73 = happyReduce 4 37 happyReduction_73
happyReduction_73 ((HappyAbsSyn30  happy_var_4) `HappyStk`
	(HappyTerminal (TokenIdentifier happy_var_3)) `HappyStk`
	_ `HappyStk`
	_ `HappyStk`
	happyRest)
	 = HappyAbsSyn37
		 (CreateIndex happy_var_3 happy_var_4
	) `HappyStk` happyRest

happyNewToken action sts stk [] =
	action 84 84 notHappyAtAll (HappyState action) sts stk []

happyNewToken action sts stk (tk:tks) =
	let cont i = action i i tk (HappyState action) sts stk tks in
	case tk of {
	TokenCreate -> cont 38;
	TokenDrop -> cont 39;
	TokenSelect -> cont 40;
	TokenDelete -> cont 41;
	TokenInsert -> cont 42;
	TokenUpdate -> cont 43;
	TokenFrom -> cont 44;
	TokenWhere -> cont 45;
	TokenSemicolon -> cont 46;
	TokenComma -> cont 47;
	TokenIdentifier happy_dollar_dollar -> cont 48;
	TokenString happy_dollar_dollar -> cont 49;
	TokenInteger happy_dollar_dollar -> cont 50;
	TokenDatabase -> cont 51;
	TokenTable -> cont 52;
	TokenIndex -> cont 53;
	TokenUse -> cont 54;
	TokenOB -> cont 55;
	TokenCB -> cont 56;
	TokenShow -> cont 57;
	TokenDatabases -> cont 58;
	TokenInt -> cont 59;
	TokenChar -> cont 60;
	TokenFloat -> cont 61;
	TokenDate -> cont 62;
	TokenPrimary -> cont 63;
	TokenKey -> cont 64;
	TokenNot -> cont 65;
	TokenNull -> cont 66;
	TokenTables -> cont 67;
	TokenInto -> cont 68;
	TokenValues -> cont 69;
	TokenSet -> cont 70;
	TokenIs -> cont 71;
	TokenDesc -> cont 72;
	TokenAsc -> cont 73;
	TokenAnd -> cont 74;
	TokenForeign -> cont 75;
	TokenReferences -> cont 76;
	TokenEq -> cont 77;
	TokenNeq -> cont 78;
	TokenLeq -> cont 79;
	TokenGeq -> cont 80;
	TokenLt -> cont 81;
	TokenGt -> cont 82;
	TokenDot -> cont 83;
	_ -> happyError' ((tk:tks), [])
	}

happyError_ explist 84 tk tks = happyError' (tks, explist)
happyError_ explist _ tk tks = happyError' ((tk:tks), explist)

newtype HappyIdentity a = HappyIdentity a
happyIdentity = HappyIdentity
happyRunIdentity (HappyIdentity a) = a

instance Functor HappyIdentity where
    fmap f (HappyIdentity a) = HappyIdentity (f a)

instance Applicative HappyIdentity where
    pure  = HappyIdentity
    (<*>) = ap
instance Monad HappyIdentity where
    return = pure
    (HappyIdentity p) >>= q = q p

happyThen :: () => HappyIdentity a -> (a -> HappyIdentity b) -> HappyIdentity b
happyThen = (>>=)
happyReturn :: () => a -> HappyIdentity a
happyReturn = (return)
happyThen1 m k tks = (>>=) m (\a -> k a tks)
happyReturn1 :: () => a -> b -> HappyIdentity a
happyReturn1 = \a tks -> (return) a
happyError' :: () => ([(Token)], [String]) -> HappyIdentity a
happyError' = HappyIdentity . (\(tokens, _) -> parseError tokens)
parseTokens tks = happyRunIdentity happySomeParser where
 happySomeParser = happyThen (happyParse action_0 tks) (\x -> case x of {HappyAbsSyn4 z -> happyReturn z; _other -> notHappyAtAll })

happySeq = happyDontSeq


parseError :: [Token]->a
parseError _=error "Error while parsing SQL statement."

isText :: Char->Bool
isText c = (isAlphaNum c || c=='_' || c=='-')

data Token
    = TokenCreate|TokenDrop|TokenSelect|TokenUpdate|TokenDelete|TokenInsert
    |TokenFrom|TokenWhere|TokenSemicolon|TokenComma
    |TokenIdentifier String|TokenString String|TokenInteger Int|TokenDatabase|TokenTable|TokenIndex|TokenOB|TokenCB|TokenUse|TokenShow|TokenDatabases
    |TokenInt|TokenChar|TokenFloat|TokenDate|TokenPrimary|TokenKey|TokenNot|TokenNull
    |TokenTables|TokenInto|TokenValues|TokenSet|TokenIs|TokenDesc|TokenAsc|TokenAnd|TokenForeign|TokenReferences
    |TokenDot|TokenEq|TokenNeq|TokenLeq|TokenGeq|TokenLt|TokenGt
    deriving(Show)
data Op=OpEq|OpNeq|OpLeq|OpGeq|OpLt|OpGt deriving (Show)
data Column=LocalCol String|ForeignCol String String deriving (Show)
data Expr=ExprV DT.TValue | ExprC Column deriving (Show)
data WhereClause=WhereOp Column Op Expr|WhereIsNull Bool | WhereAnd WhereClause WhereClause deriving (Show)

type ForeignRef=(Column, Column) 
data TableDescription = TableDescription {params :: [DT.TParam], primary :: Maybe String, fkey:: [ForeignRef]} deriving (Show)
appendParam :: DT.TParam->TableDescription->TableDescription
appendParam par d=d {params=par:(params d)}
appendForeign :: String->String->String->TableDescription->TableDescription
appendForeign fld tbl col d=d{fkey=(LocalCol fld, ForeignCol tbl col):(fkey d)}
setPrimary :: String->TableDescription->TableDescription
setPrimary pri d=if isNothing (primary d) then d{primary=Just pri} else error "Duplicate PrimaryKey!"
data SQLStatement
    = CreateDatabase String
    | DropDatabase String
    | CreateTable String TableDescription
    | DropTable String
    | UseDatabase String
    | ShowDatabases
    | ShowDatabase
    | SelectStmt [Column] [String] WhereClause
    | UpdateStmt String [(String, DT.TValue)] WhereClause
    | DeleteStmt String WhereClause
    | InsertStmt String [DT.TValue]
    | CreateIndex String [String]
    | DropIndex String String
    | ShowTables
    | DescribeTable String
    deriving (Show)

tokenize :: String->[Token]
tokenize []=[]
tokenize (c:cs)
    | isSpace c=tokenize cs
    | isDigit c=readNum $ c:cs
    | isText c=readVar $ c:cs
tokenize (',':cs)=TokenComma:tokenize cs
tokenize (';':cs)=TokenSemicolon:tokenize cs
tokenize ('(':cs)=TokenOB:tokenize cs
tokenize (')':cs)=TokenCB:tokenize cs
tokenize ('\'':cs)=let (a, b)=span (/='\'') cs in (TokenString $ init a):tokenize ((last a):b)
tokenize ('=':cs)=TokenEq:tokenize cs
tokenize ('<':'=':cs)=TokenLeq:tokenize cs
tokenize ('>':'=':cs)=TokenGeq:tokenize cs
tokenize ('<':'>':cs)=TokenNeq:tokenize cs
tokenize ('<':cs)=TokenLt:tokenize cs
tokenize ('>':cs)=TokenGt:tokenize cs
tokenize ('.':cs)=TokenDot:tokenize cs
readVar cs=
    let (token, rest)=span isText cs
        sym=case token of
                "database"->TokenDatabase
                "databases"->TokenDatabases
                "table"->TokenTable
                "tables"->TokenTables
                "show"->TokenShow
                "create"-> TokenCreate
                "drop"-> TokenDrop
                "use"->TokenUse
                "primary"->TokenPrimary
                "key"->TokenKey
                "not"->TokenNot
                "null"->TokenNull
                "insert"->TokenInsert
                "into"->TokenInto
                "values"->TokenValues
                "delete"-> TokenDelete
                "from"->TokenFrom
                "where"->TokenWhere
                "update"-> TokenUpdate
                "set"->TokenSet
                "select"-> TokenSelect
                "is"->TokenIs
                "int"->TokenInt
                "varchar"->TokenChar
                "desc"->TokenDesc
                "asc"->TokenAsc
                "index"->TokenIndex
                "and"->TokenAnd
                "date"->TokenDate
                "float"->TokenFloat
                "foreign"->TokenForeign
                "references"->TokenReferences
                
                

                otherwise->TokenIdentifier token
    in sym:tokenize rest
readNum cs=TokenInteger (read num):tokenize rest
        where (num, rest)=span isDigit cs
parseSQL=parseTokens . tokenize
{-# LINE 1 "templates\GenericTemplate.hs" #-}
{-# LINE 1 "templates\\\\GenericTemplate.hs" #-}
{-# LINE 1 "<built-in>" #-}
{-# LINE 1 "<command-line>" #-}
{-# LINE 8 "<command-line>" #-}
{-# LINE 1 "D:/GitHub/haskell-platform/build/ghc-bindist/local/lib/include/ghcversion.h" #-}















{-# LINE 8 "<command-line>" #-}
{-# LINE 1 "F:/Users/randy/AppData/Local/Temp/ghc15608_0/ghc_2.h" #-}














































































































































































{-# LINE 8 "<command-line>" #-}
{-# LINE 1 "templates\\\\GenericTemplate.hs" #-}
-- Id: GenericTemplate.hs,v 1.26 2005/01/14 14:47:22 simonmar Exp 









{-# LINE 43 "templates\\\\GenericTemplate.hs" #-}

data Happy_IntList = HappyCons Int Happy_IntList







{-# LINE 65 "templates\\\\GenericTemplate.hs" #-}

{-# LINE 75 "templates\\\\GenericTemplate.hs" #-}

{-# LINE 84 "templates\\\\GenericTemplate.hs" #-}

infixr 9 `HappyStk`
data HappyStk a = HappyStk a (HappyStk a)

-----------------------------------------------------------------------------
-- starting the parse

happyParse start_state = happyNewToken start_state notHappyAtAll notHappyAtAll

-----------------------------------------------------------------------------
-- Accepting the parse

-- If the current token is (1), it means we've just accepted a partial
-- parse (a %partial parser).  We must ignore the saved token on the top of
-- the stack in this case.
happyAccept (1) tk st sts (_ `HappyStk` ans `HappyStk` _) =
        happyReturn1 ans
happyAccept j tk st sts (HappyStk ans _) = 
         (happyReturn1 ans)

-----------------------------------------------------------------------------
-- Arrays only: do the next action

{-# LINE 137 "templates\\\\GenericTemplate.hs" #-}

{-# LINE 147 "templates\\\\GenericTemplate.hs" #-}
indexShortOffAddr arr off = arr Happy_Data_Array.! off


{-# INLINE happyLt #-}
happyLt x y = (x < y)






readArrayBit arr bit =
    Bits.testBit (indexShortOffAddr arr (bit `div` 16)) (bit `mod` 16)






-----------------------------------------------------------------------------
-- HappyState data type (not arrays)



newtype HappyState b c = HappyState
        (Int ->                    -- token number
         Int ->                    -- token number (yes, again)
         b ->                           -- token semantic value
         HappyState b c ->              -- current state
         [HappyState b c] ->            -- state stack
         c)



-----------------------------------------------------------------------------
-- Shifting a token

happyShift new_state (1) tk st sts stk@(x `HappyStk` _) =
     let i = (case x of { HappyErrorToken (i) -> i }) in
--     trace "shifting the error token" $
     new_state i i tk (HappyState (new_state)) ((st):(sts)) (stk)

happyShift new_state i tk st sts stk =
     happyNewToken new_state ((st):(sts)) ((HappyTerminal (tk))`HappyStk`stk)

-- happyReduce is specialised for the common cases.

happySpecReduce_0 i fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happySpecReduce_0 nt fn j tk st@((HappyState (action))) sts stk
     = action nt j tk st ((st):(sts)) (fn `HappyStk` stk)

happySpecReduce_1 i fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happySpecReduce_1 nt fn j tk _ sts@(((st@(HappyState (action))):(_))) (v1`HappyStk`stk')
     = let r = fn v1 in
       happySeq r (action nt j tk st sts (r `HappyStk` stk'))

happySpecReduce_2 i fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happySpecReduce_2 nt fn j tk _ ((_):(sts@(((st@(HappyState (action))):(_))))) (v1`HappyStk`v2`HappyStk`stk')
     = let r = fn v1 v2 in
       happySeq r (action nt j tk st sts (r `HappyStk` stk'))

happySpecReduce_3 i fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happySpecReduce_3 nt fn j tk _ ((_):(((_):(sts@(((st@(HappyState (action))):(_))))))) (v1`HappyStk`v2`HappyStk`v3`HappyStk`stk')
     = let r = fn v1 v2 v3 in
       happySeq r (action nt j tk st sts (r `HappyStk` stk'))

happyReduce k i fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happyReduce k nt fn j tk st sts stk
     = case happyDrop (k - ((1) :: Int)) sts of
         sts1@(((st1@(HappyState (action))):(_))) ->
                let r = fn stk in  -- it doesn't hurt to always seq here...
                happyDoSeq r (action nt j tk st1 sts1 r)

happyMonadReduce k nt fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happyMonadReduce k nt fn j tk st sts stk =
      case happyDrop k ((st):(sts)) of
        sts1@(((st1@(HappyState (action))):(_))) ->
          let drop_stk = happyDropStk k stk in
          happyThen1 (fn stk tk) (\r -> action nt j tk st1 sts1 (r `HappyStk` drop_stk))

happyMonad2Reduce k nt fn (1) tk st sts stk
     = happyFail [] (1) tk st sts stk
happyMonad2Reduce k nt fn j tk st sts stk =
      case happyDrop k ((st):(sts)) of
        sts1@(((st1@(HappyState (action))):(_))) ->
         let drop_stk = happyDropStk k stk





             _ = nt :: Int
             new_state = action

          in
          happyThen1 (fn stk tk) (\r -> happyNewToken new_state sts1 (r `HappyStk` drop_stk))

happyDrop (0) l = l
happyDrop n ((_):(t)) = happyDrop (n - ((1) :: Int)) t

happyDropStk (0) l = l
happyDropStk n (x `HappyStk` xs) = happyDropStk (n - ((1)::Int)) xs

-----------------------------------------------------------------------------
-- Moving to a new state after a reduction

{-# LINE 267 "templates\\\\GenericTemplate.hs" #-}
happyGoto action j tk st = action j j tk (HappyState action)


-----------------------------------------------------------------------------
-- Error recovery ((1) is the error token)

-- parse error if we are in recovery and we fail again
happyFail explist (1) tk old_st _ stk@(x `HappyStk` _) =
     let i = (case x of { HappyErrorToken (i) -> i }) in
--      trace "failing" $ 
        happyError_ explist i tk

{-  We don't need state discarding for our restricted implementation of
    "error".  In fact, it can cause some bogus parses, so I've disabled it
    for now --SDM

-- discard a state
happyFail  (1) tk old_st (((HappyState (action))):(sts)) 
                                                (saved_tok `HappyStk` _ `HappyStk` stk) =
--      trace ("discarding state, depth " ++ show (length stk))  $
        action (1) (1) tk (HappyState (action)) sts ((saved_tok`HappyStk`stk))
-}

-- Enter error recovery: generate an error token,
--                       save the old token and carry on.
happyFail explist i tk (HappyState (action)) sts stk =
--      trace "entering error recovery" $
        action (1) (1) tk (HappyState (action)) sts ( (HappyErrorToken (i)) `HappyStk` stk)

-- Internal happy errors:

notHappyAtAll :: a
notHappyAtAll = error "Internal Happy error\n"

-----------------------------------------------------------------------------
-- Hack to get the typechecker to accept our action functions







-----------------------------------------------------------------------------
-- Seq-ing.  If the --strict flag is given, then Happy emits 
--      happySeq = happyDoSeq
-- otherwise it emits
--      happySeq = happyDontSeq

happyDoSeq, happyDontSeq :: a -> b -> b
happyDoSeq   a b = a `seq` b
happyDontSeq a b = b

-----------------------------------------------------------------------------
-- Don't inline any functions from the template.  GHC has a nasty habit
-- of deciding to inline happyGoto everywhere, which increases the size of
-- the generated parser quite a bit.

{-# LINE 333 "templates\\\\GenericTemplate.hs" #-}
{-# NOINLINE happyShift #-}
{-# NOINLINE happySpecReduce_0 #-}
{-# NOINLINE happySpecReduce_1 #-}
{-# NOINLINE happySpecReduce_2 #-}
{-# NOINLINE happySpecReduce_3 #-}
{-# NOINLINE happyReduce #-}
{-# NOINLINE happyMonadReduce #-}
{-# NOINLINE happyGoto #-}
{-# NOINLINE happyFail #-}

-- end of Happy Template.
