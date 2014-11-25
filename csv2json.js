var fs = require('fs');
var csv = require('csv');
var head = true;
var map = {};

function skillLevelToString(level) {
  var arr = ['Beginner',
             'Intermediate',
             'Advanced',
             'Expert',
             'Champion',
             'World Class' ];
  return arr[level - 1];
}

function makeObject(record) {
  var obj = {};
  for (key in map) {
    obj[key] = record[map[key]];
  }
  return obj;
}

function parseDate(dateString) {
  var match = /^(\d\d)\/(\d\d)\/(\d\d)\s*(\d\d)\:(\d\d)\:(\d\d)/.exec(dateString);
  if (! match) { return null; }
  var year = Number(match[3]);
  year += (year > 50 ? 1900 : 2000);
  return new Date(year, Number(match[2]) - 1, Number(match[1]), Number(match[4]), Number(match[5]), Number(match[6]));
}

function getName(name, surname) {
  name = name.trim();
  surname = surname.trim();
  return (name == surname ? name : name + ' ' + surname)
}

fs.createReadStream(__dirname + '/members.csv')
  .pipe(csv.parse())
  .pipe(csv.transform (function(record){
                         if(head) {
                           for (var i = 0; i < record.length; ++i) {
                             map[record[i]] = i;
                           }
                           head = false;
                         }
                         else {
                           record = makeObject(record);
                           var rec = {
                             bboName: record.mBBOLoginName,
                             name: getName(record.mName, record.mSurname),
                             nation: record.mCountry,
                             email: record.mEMail,
                             level: skillLevelToString(record.mSkillLevel),
                             isStarPlayer: record.mStarPlayers === '1',
                             isRbdPlayer: record.mTopPlayers,
                             isEnabled: record.mValid === '1' && record.mDisable === '0',
                             isBlackListed: record.mBlackList === '1',
                             isBanned: false,
                             rock: {
                               totalScores: {
                                numTournaments: parseInt(record.mNumberOfRankedTournaments, 10),
                                averageScore: parseFloat(record.mRankAverage),
                                averageMatchPoints: parseFloat(record.mAverageScore),
                                awards: parseInt(record.mRank, 10)
                               } 
                             },
                             rbd: {
                               totalScores: {
                                numTournaments: parseInt(record.mTPNumberOfRankedTournaments, 10),
                                averageScore: parseFloat(record.mTPRankAverage),
                                averageMatchPoints: parseFloat(record.mTPAverageScore),
                                awards: parseInt(record.mTPRank, 10)
                               } 
                             },
                             validatedAt: parseDate(record.mValidateDate),
                             registeredAt: parseDate(record.mCheckRegistrationDate),
                             createdAt: parseDate(record.mRegisterDate)
                           };

                           console.log(JSON.stringify(rec));
                           console.log(',');
                         };
                       }));

/* CSV Input:
mID,mBBOLoginName,mValid,mValidateDate,mDisable,mRegisterDate,mCheckRegistration,mCheckRegistrationDate,
mBlackList,mBLExpiredDate,mBLDate,mName,mSurname,mCountry,mEMail,mSkillLevel,mTelephone,
mQuestion1,mQuestion2,mQuestion3,mQuestion4,
mTournaments1,mTournaments2,mTournaments3,mTournaments4,mTournaments5,mTournaments6,mTournaments7,
mSuggestions,m3AM,m7AM,m11AM,m3PM,m7PM,m11PM,mNote,
mAverageScore,mNumberOfTournaments,mRank,mNumberOfRankedTournaments,mRankAverage,mLastGameDate,
mTopPlayers,mStarPlayers,mTPIssuedBy,
mTPAverageScore,mTPNumberOfTournaments,mTPRank,mTPNumberOfRankedTournaments,mTPRankAverage,mTPLastGameDate
*/

/* JSON output:
{
  bboName             : mBBOLoginName,
  name                : mName + ' ' + mSurname,
  nation              : mCountry,
  email               : mEMail,
  level               : mSkillLevel (1 = 'Beginner', 2 = 'Intermediate', 3 = 'Advanced', 4 = 'Expert', 5 = 'Champion', 6 = 'World Class')
  isStarPlayer        : mStarPlayers,
  isRbdPlayer         : mTopPlayers,
  isEnabled           : ! mDisable,
  isBlackListed       : mBlackList,
  isBanned            : false,
  rock: {
    totalScores         : {
      numTournaments      : mNumberOfRankedTournaments,
      averageScore        : mRankAverage,
      averageMatchPoints  : mAverageScore,
      awards              : mRank}
  },
  rbd: {
    totalScores         : {
      numTournaments      : mTPNumberOfRankedTournaments,
      averageScore        : mTPRankAverage,
      averageMatchPoints  : mTPAverageScore,
      awards              : mTPRank}
  },
  validatedAt         : mValidateDate,
  registeredAt        : mCheckRegistrationDate,
  createdAt           : mRegisterDate
}
*/
